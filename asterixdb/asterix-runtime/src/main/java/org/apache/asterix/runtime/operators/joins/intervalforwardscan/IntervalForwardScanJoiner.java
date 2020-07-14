/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.intervalforwardscan;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.runtime.operators.joins.IIntervalJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.IStreamJoiner;
import org.apache.hyracks.dataflow.std.join.JoinData;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

class IntervalSideTuple {
    // Tuple access
    int fieldId;
    ITupleAccessor accessor;
    int tupleIndex;
    int frameIndex = -1;

    // Join details
    final IIntervalJoinChecker imjc;

    // Interval details
    long start;
    long end;

    public IntervalSideTuple(IIntervalJoinChecker imjc, ITupleAccessor accessor, int fieldId) {
        this.imjc = imjc;
        this.accessor = accessor;
        this.fieldId = fieldId;
    }

    public void setTuple(TuplePointer tp) {
        if (frameIndex != tp.getFrameIndex()) {
            accessor.reset(tp);
            frameIndex = tp.getFrameIndex();
        }
        tupleIndex = tp.getTupleIndex();
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public void loadTuple() {
        tupleIndex = accessor.getTupleId();
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public ITupleAccessor getAccessor() {
        return accessor;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean hasMoreMatches(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkIfMoreMatches(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean compareJoin(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(accessor, tupleIndex, ist.accessor, ist.tupleIndex, false);
    }

    public boolean removeFromMemory(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToRemoveInMemory(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean checkForEarlyExit(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkForEarlyExit(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

}

/**
 * Interval Forward Sweep Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The both right and left use memory to maintain active intervals for the join.
 */
public class IntervalForwardScanJoiner implements IStreamJoiner {

    public enum TupleStatus {
        UNKNOWN,
        LOADED,
        EMPTY;

        public boolean isLoaded() {
            return this.equals(LOADED);
        }

        public boolean isEmpty() {
            return this.equals(EMPTY);
        }

        public boolean isKnown() {
            return !this.equals(UNKNOWN);
        }
    }

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    private final IPartitionedDeletableTupleBufferManager bufferManager;
    private final IFrameWriter writer;

    private final ForwardScanActiveManager[] activeManager;
    private final ITupleAccessor[] memoryAccessor;
    private final RunFileStream[] runFileStream;
    private final RunFilePointer[] runFilePointer;

    private IntervalSideTuple[] memoryTuple;
    private IntervalSideTuple[] inputTuple;

    private final IIntervalJoinChecker imjc;
    private final IIntervalJoinChecker imjcInverse;

    private final int leftKey;
    private final int rightKey;

    private final LinkedList<TuplePointer> processingGroup = new LinkedList<>();
    private static final LinkedList<TuplePointer> empty = new LinkedList<>();

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final ITupleAccessor[] inputAccessor;
    protected final JoinData[] joinData;

    public IntervalForwardScanJoiner(IHyracksTaskContext ctx, JoinData leftJoinData, JoinData rightJoinData,
            int memorySize, IIntervalJoinCheckerFactory imjcf, int[] leftKeys, int[] rightKeys, IFrameWriter writer,
            int nPartitions) throws HyracksDataException {

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftJoinData.getRecordDescriptor());
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightJoinData.getRecordDescriptor());

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[RIGHT_PARTITION] = new VSizeFrame(ctx);

        joinData = new JoinData[JOIN_PARTITIONS];
        joinData[LEFT_PARTITION] = leftJoinData;
        joinData[RIGHT_PARTITION] = rightJoinData;

        this.writer = writer;

        this.imjc = imjcf.createIntervalMergeJoinChecker(leftKeys, rightKeys, ctx, nPartitions);
        this.imjcInverse = imjcf.createIntervalInverseMergeJoinChecker(leftKeys, rightKeys, ctx, nPartitions);

        this.leftKey = leftKeys[0];
        this.rightKey = rightKeys[0];

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[LEFT_PARTITION] = leftJoinData.getRecordDescriptor();
        recordDescriptors[RIGHT_PARTITION] = rightJoinData.getRecordDescriptor();
        bufferManager =
                new VPartitionDeletableTupleBufferManager(ctx, VPartitionDeletableTupleBufferManager.NO_CONSTRAIN,
                        JOIN_PARTITIONS, memorySize * ctx.getInitialFrameSize(), recordDescriptors);

        memoryAccessor = new ITupleAccessor[JOIN_PARTITIONS];
        memoryAccessor[LEFT_PARTITION] = bufferManager.getTupleAccessor(leftJoinData.getRecordDescriptor());
        memoryAccessor[RIGHT_PARTITION] = bufferManager.getTupleAccessor(rightJoinData.getRecordDescriptor());

        activeManager = new ForwardScanActiveManager[JOIN_PARTITIONS];
        activeManager[LEFT_PARTITION] = new ForwardScanActiveManager(bufferManager, LEFT_PARTITION);
        activeManager[RIGHT_PARTITION] = new ForwardScanActiveManager(bufferManager, RIGHT_PARTITION);

        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[LEFT_PARTITION] = leftJoinData.getRunFileStream();
        runFileStream[RIGHT_PARTITION] = rightJoinData.getRunFileStream();

        runFilePointer = new RunFilePointer[JOIN_PARTITIONS];
        runFilePointer[LEFT_PARTITION] = new RunFilePointer();
        runFilePointer[RIGHT_PARTITION] = new RunFilePointer();

        memoryTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        memoryTuple[LEFT_PARTITION] = new IntervalSideTuple(imjc, memoryAccessor[LEFT_PARTITION], leftKey);
        memoryTuple[RIGHT_PARTITION] = new IntervalSideTuple(imjcInverse, memoryAccessor[RIGHT_PARTITION], rightKey);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[LEFT_PARTITION] = new IntervalSideTuple(imjc, inputAccessor[LEFT_PARTITION], leftKey);
        inputTuple[RIGHT_PARTITION] = new IntervalSideTuple(imjcInverse, inputAccessor[RIGHT_PARTITION], rightKey);

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
    }

    private void flushMemory(int partition) throws HyracksDataException {
        activeManager[partition].clear();
    }

    private TupleStatus loadTuple(int partition) throws HyracksDataException {
        if (!inputAccessor[partition].exists() && !runFileStream[partition].loadNextBuffer(inputAccessor[partition])) {
            return TupleStatus.EMPTY;
        }
        return TupleStatus.LOADED;
    }

    public void processJoin() throws HyracksDataException {

        runFileStream[LEFT_PARTITION].startReadingRunFile(inputAccessor[LEFT_PARTITION]);
        runFileStream[RIGHT_PARTITION].startReadingRunFile(inputAccessor[RIGHT_PARTITION]);

        TupleStatus leftTs = loadTuple(LEFT_PARTITION);
        TupleStatus rightTs = loadTuple(RIGHT_PARTITION);

        while ((leftTs.isLoaded() || activeManager[LEFT_PARTITION].hasRecords())
                && (rightTs.isLoaded() || activeManager[RIGHT_PARTITION].hasRecords())) {
            processNextTuple(writer);
            leftTs = loadTuple(LEFT_PARTITION);
            rightTs = loadTuple(RIGHT_PARTITION);
        }

        resultAppender.write(writer, true);

        activeManager[LEFT_PARTITION].clear();
        activeManager[RIGHT_PARTITION].clear();
        runFileStream[LEFT_PARTITION].close();
        runFileStream[RIGHT_PARTITION].close();
        runFileStream[LEFT_PARTITION].removeRunFile();
        runFileStream[RIGHT_PARTITION].removeRunFile();
    }

    private void processNextTuple(IFrameWriter writer) throws HyracksDataException {
        // Ensure a tuple is in memory.
        TupleStatus leftTs = loadTuple(LEFT_PARTITION);
        if (leftTs.isLoaded() && !activeManager[LEFT_PARTITION].hasRecords()) {
            TuplePointer tp = activeManager[LEFT_PARTITION].addTuple(inputAccessor[LEFT_PARTITION]);
            if (tp == null) {
                // Should never happen if memory budget is correct.
                throw new HyracksDataException("Left partition does not have access to a single page of memory.");
            }
            inputAccessor[LEFT_PARTITION].next();
        }
        TupleStatus rightTs = loadTuple(RIGHT_PARTITION);
        if (rightTs.isLoaded() && !activeManager[RIGHT_PARTITION].hasRecords()) {
            TuplePointer tp = activeManager[RIGHT_PARTITION].addTuple(inputAccessor[RIGHT_PARTITION]);
            if (tp == null) {
                // Should never happen if memory budget is correct.
                throw new HyracksDataException("Right partition does not have access to a single page of memory.");
            }
            inputAccessor[RIGHT_PARTITION].next();
        }
        // If both sides have value in memory, run join.
        if (activeManager[LEFT_PARTITION].hasRecords() && activeManager[RIGHT_PARTITION].hasRecords()) {
            if (checkToProcessRightTuple()) {
                // Right side from stream
                processTuple(RIGHT_PARTITION, LEFT_PARTITION, writer);
            } else {
                // Left side from stream
                processTuple(LEFT_PARTITION, RIGHT_PARTITION, writer);
            }
        }
    }

    private boolean checkToProcessRightTuple() {
        TuplePointer leftTp = activeManager[LEFT_PARTITION].getFirst();
        memoryAccessor[LEFT_PARTITION].reset(leftTp);
        long leftStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[LEFT_PARTITION], leftTp.getTupleIndex(), leftKey);

        TuplePointer rightTp = activeManager[RIGHT_PARTITION].getFirst();
        memoryAccessor[RIGHT_PARTITION].reset(rightTp);
        long rightStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[RIGHT_PARTITION], rightTp.getTupleIndex(), rightKey);
        return leftStart > rightStart;
    }

    private void processTupleSpill(int main, int other, boolean reversed, IFrameWriter writer)
            throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        TupleStatus ts = loadTuple(main);
        while (ts.isLoaded() && activeManager[other].hasRecords()) {
            inputTuple[main].loadTuple();
            processTupleJoin(inputTuple[main], other, reversed, writer, empty);
            inputAccessor[main].next();
            ts = loadTuple(main);
        }
    }

    private void processTuple(int main, int other, IFrameWriter writer) throws HyracksDataException {
        // Check tuple with all memory.
        // Purge as processing
        // Added new items from right to memory and check
        if (!activeManager[main].hasRecords()) {
            return;
        }
        TuplePointer searchTp = activeManager[main].getFirst();
        TuplePointer searchEndTp = searchTp;

        processingGroup.clear();
        processingGroup.add(searchTp);

        processSingleWithMemory(other, main, searchTp, writer);

        // Add tuples from the stream.
        processGroupWithStream(other, main, searchEndTp, writer);

        // Remove search tuple
        for (Iterator<TuplePointer> groupIterator = processingGroup.iterator(); groupIterator.hasNext();) {
            TuplePointer groupTp = groupIterator.next();
            activeManager[main].remove(groupTp);
        }
    }

    private void processGroupWithStream(int outer, int inner, TuplePointer searchEndTp, IFrameWriter writer)
            throws HyracksDataException {

        // Add tuples from the stream.
        while (loadTuple(outer).isLoaded()) {
            inputTuple[outer].loadTuple();
            memoryTuple[inner].setTuple(searchEndTp);
            if (!memoryTuple[inner].hasMoreMatches(inputTuple[outer])) {
                break;
            }
            TuplePointer tp = activeManager[outer].addTuple(inputAccessor[outer]);
            if (tp != null) {
                memoryTuple[outer].setTuple(tp);

                // Search group.
                for (Iterator<TuplePointer> groupIterator = processingGroup.iterator(); groupIterator.hasNext();) {
                    TuplePointer groupTp = groupIterator.next();
                    memoryTuple[inner].setTuple(groupTp);

                    // Add to result if matched.
                    if (memoryTuple[LEFT_PARTITION].compareJoin(memoryTuple[RIGHT_PARTITION])) {
                        //                      memoryAccessor[LEFT_PARTITION].reset(groupTp);
                        addToResult(memoryTuple[LEFT_PARTITION].getAccessor(),
                                memoryTuple[LEFT_PARTITION].getTupleIndex(), memoryTuple[RIGHT_PARTITION].getAccessor(),
                                memoryTuple[RIGHT_PARTITION].getTupleIndex(), false, writer);
                    }
                }
            } else {
                // Spill case, remove search tuple before freeze.
                freezeAndStartSpill(writer, processingGroup);
                processingGroup.clear();
                return;
            }
            inputAccessor[outer].next();
        }
    }

    private void processSingleWithMemory(int outer, int inner, TuplePointer searchTp, IFrameWriter writer)
            throws HyracksDataException {
        memoryTuple[inner].setTuple(searchTp);

        // Compare with tuple in memory
        for (Iterator<TuplePointer> iterator = activeManager[outer].getIterator(); iterator.hasNext();) {
            TuplePointer matchTp = iterator.next();
            memoryTuple[outer].setTuple(matchTp);
            if (memoryTuple[inner].removeFromMemory(memoryTuple[outer])) {
                // Remove if the tuple no long matches.
                activeManager[outer].remove(iterator, matchTp);
            } else if (memoryTuple[inner].checkForEarlyExit(memoryTuple[outer])) {
                // No more possible comparisons
                break;
            } else if (memoryTuple[LEFT_PARTITION].compareJoin(memoryTuple[RIGHT_PARTITION])) {
                // Add to result if matched.
                addToResult(memoryTuple[LEFT_PARTITION].getAccessor(), memoryTuple[LEFT_PARTITION].getTupleIndex(),
                        memoryTuple[RIGHT_PARTITION].getAccessor(), memoryTuple[RIGHT_PARTITION].getTupleIndex(), false,
                        writer);
            }
        }
    }

    private void processInMemoryJoin(int outer, int inner, boolean reversed, IFrameWriter writer,
            LinkedList<TuplePointer> searchGroup) throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> outerIterator = activeManager[outer].getIterator(); outerIterator.hasNext();) {
            TuplePointer outerTp = outerIterator.next();
            if (searchGroup.contains(outerTp)) {
                continue;
            }
            memoryTuple[outer].setTuple(outerTp);
            processTupleJoin(memoryTuple[outer], inner, reversed, writer, searchGroup);
        }
    }

    private void processTupleJoin(IntervalSideTuple testTuple, int inner, boolean reversed, IFrameWriter writer,
            LinkedList<TuplePointer> searchGroup) throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> innerIterator = activeManager[inner].getIterator(); innerIterator.hasNext();) {
            TuplePointer innerTp = innerIterator.next();
            if (searchGroup.contains(innerTp)) {
                continue;
            }
            memoryTuple[inner].setTuple(innerTp);

            if (testTuple.removeFromMemory(memoryTuple[inner])) {
                // Remove if the tuple no long matches.
                activeManager[inner].remove(innerIterator, innerTp);
            } else if (!testTuple.hasMoreMatches(memoryTuple[inner])) {
                // Exit if no more possible matches
                break;
            } else if (testTuple.compareJoin(memoryTuple[inner])) {
                // Add to result if matched.
                addToResult(testTuple.getAccessor(), testTuple.getTupleIndex(), memoryTuple[inner].getAccessor(),
                        memoryTuple[inner].getTupleIndex(), reversed, writer);
            }
        }
    }

    private void freezeAndClearMemory(IFrameWriter writer, LinkedList<TuplePointer> searchGroup)
            throws HyracksDataException {

        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            processInMemoryJoin(RIGHT_PARTITION, LEFT_PARTITION, true, writer, searchGroup);
        } else {
            processInMemoryJoin(LEFT_PARTITION, RIGHT_PARTITION, false, writer, searchGroup);
        }
    }

    private void freezeAndStartSpill(IFrameWriter writer, LinkedList<TuplePointer> searchGroup)
            throws HyracksDataException {
        int freezePartition;
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            freezePartition = RIGHT_PARTITION;
        } else {
            freezePartition = LEFT_PARTITION;
        }
        // Mark where to start reading
        runFilePointer[freezePartition].reset(runFileStream[freezePartition].getReadPointer(),
                inputAccessor[freezePartition].getTupleId());
        // Start writing
        runFileStream[freezePartition].startRunFileWriting();
        freezeAndClearMemory(writer, searchGroup);
        // Frozen.
        if (runFileStream[LEFT_PARTITION].isWriting()) {
            processTupleSpill(LEFT_PARTITION, RIGHT_PARTITION, false, writer);
            // Memory is empty and we can start processing the run file.
            unfreezeAndContinue(LEFT_PARTITION, inputAccessor[LEFT_PARTITION]);
        } else if (runFileStream[RIGHT_PARTITION].isWriting()) {
            processTupleSpill(RIGHT_PARTITION, LEFT_PARTITION, true, writer);
            // Memory is empty and we can start processing the run file.
            unfreezeAndContinue(RIGHT_PARTITION, inputAccessor[RIGHT_PARTITION]);
        }
    }

    private void unfreezeAndContinue(int frozenPartition, ITupleAccessor accessor) throws HyracksDataException {
        int flushPartition = frozenPartition == LEFT_PARTITION ? RIGHT_PARTITION : LEFT_PARTITION;
        runFileStream[frozenPartition].flushRunFile();

        // Clear memory
        flushMemory(flushPartition);
        runFileStream[frozenPartition].startReadingRunFile(accessor, runFilePointer[frozenPartition].getFileOffset());
        accessor.setTupleId(runFilePointer[frozenPartition].getTupleIndex());
        runFilePointer[frozenPartition].reset(-1, -1);

    }

}
