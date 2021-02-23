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
package org.apache.asterix.runtime.operators.joins.interval.TimeSweep;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.interval.TimeSweep.memory.VPartitionDeletableTupleBufferManager;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalSideTuple;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Interval Index Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The both right and left use memory to maintain active intervals for the join.
 */
public class IntervalTimeSweepJoiner {

    private static final Logger LOGGER = Logger.getLogger(IntervalTimeSweepJoiner.class.getName());

    private final ActiveSweepManager[] activeManager;
    private final byte point;

    private final IDeallocatableFramePool framePool;
    private final VPartitionDeletableTupleBufferManager bufferManager;
    private final ITupleAccessor[] memoryAccessor;
    private final LinkedList<TuplePointer> memoryBuffer = new LinkedList<>();

    //private final RunFileStream runFileStream;
    private final RunFileStream[] runFileStream;
    private final RunFilePointer[] runFilePointer;
    private final int[] streamIndex;

    //private IntervalSideTuple memoryTuple;
    private IntervalSideTuple[] inputTuple;

    private final int buildKey;
    private final int probeKey;

    private final IIntervalJoinUtil iju;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final FrameTupleCursor[] inputCursor;

    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private long leftSpillCount = 0;
    private long rightSpillCount = 0;
    private long[] spillFileCount = { 0, 0 };
    private long[] spillReadCount = { 0, 0 };
    private long[] spillWriteCount = { 0, 0 };
    private long[] joinSideCount = { 0, 0 };

    public IntervalTimeSweepJoiner(IHyracksTaskContext ctx, int memorySize, IIntervalJoinUtil iju, int buildKey,
            int probeKey, RecordDescriptor buildRd, RecordDescriptor probeRd,
            Comparator<EndPointIndexItem> endPointComparator) throws HyracksDataException {
        this.iju = iju;

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "IntervalMergeJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        inputCursor[PROBE_PARTITION] = new FrameTupleCursor(probeRd);

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[BUILD_PARTITION] = buildRd;
        recordDescriptors[PROBE_PARTITION] = probeRd;

        streamIndex = new int[JOIN_PARTITIONS];
        streamIndex[BUILD_PARTITION] = -2;
        streamIndex[PROBE_PARTITION] = -2;

        //Two frames are used for the runfile stream, and one frame for each input (2 outputs).
        framePool = new DeallocatableFramePool(ctx, (memorySize - 4) * ctx.getInitialFrameSize());
        bufferManager =
                new VPartitionDeletableTupleBufferManager(ctx, VPartitionDeletableTupleBufferManager.NO_CONSTRAIN,
                        JOIN_PARTITIONS, memorySize * ctx.getInitialFrameSize(), recordDescriptors);
        memoryAccessor = new ITupleAccessor[JOIN_PARTITIONS];
        memoryAccessor[PROBE_PARTITION] = bufferManager.getTupleAccessor(probeRd);
        memoryAccessor[BUILD_PARTITION] = bufferManager.getTupleAccessor(buildRd);

        // Run File and frame cache (build buffer)
        //runFileStream = new RunFileStream(ctx, "imj-build");
        runFilePointer = new RunFilePointer[JOIN_PARTITIONS];
        runFilePointer[PROBE_PARTITION] = new RunFilePointer();
        runFilePointer[BUILD_PARTITION] = new RunFilePointer();
        //runFileStream.createRunFileWriting();
        //runFileStream.startRunFileWriting();
        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[BUILD_PARTITION] = new RunFileStream(ctx, "imj-build");
        runFileStream[PROBE_PARTITION] = new RunFileStream(ctx, "itsj-probe");

        runFileStream[BUILD_PARTITION].createRunFileWriting();
        runFileStream[BUILD_PARTITION].startRunFileWriting();
        runFileStream[PROBE_PARTITION].createRunFileWriting();
        runFileStream[PROBE_PARTITION].startRunFileWriting();

        //memoryTuple = new IntervalSideTuple(this.iju, memoryAccessor, probeKey);

        this.point = true ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;
        activeManager = new ActiveSweepManager[JOIN_PARTITIONS];
        activeManager[BUILD_PARTITION] =
                new ActiveSweepManager(bufferManager, buildKey, BUILD_PARTITION, endPointComparator, point);
        activeManager[PROBE_PARTITION] =
                new ActiveSweepManager(bufferManager, probeKey, PROBE_PARTITION, endPointComparator, point);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[PROBE_PARTITION] = new IntervalSideTuple(this.iju, inputCursor[PROBE_PARTITION], probeKey);
        inputTuple[BUILD_PARTITION] = new IntervalSideTuple(this.iju, inputCursor[BUILD_PARTITION], buildKey);

        this.buildKey = buildKey;
        this.probeKey = probeKey;

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputCursor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[BUILD_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream[BUILD_PARTITION].addToRunFile(inputCursor[BUILD_PARTITION].getAccessor(), x);
        }
    }

    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        inputCursor[PROBE_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[PROBE_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream[PROBE_PARTITION].addToRunFile(inputCursor[PROBE_PARTITION].getAccessor(), x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
        runFileStream[BUILD_PARTITION].flushRunFile();
        runFileStream[BUILD_PARTITION].startReadingRunFile(inputCursor[BUILD_PARTITION]);
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {
        runFileStream[PROBE_PARTITION].flushRunFile();
        runFileStream[PROBE_PARTITION].startReadingRunFile(inputCursor[PROBE_PARTITION]);
        processJoin(writer);
    }

    private boolean loadTuple(int partition) {
        if (inputCursor[partition].hasNext()) {
            inputCursor[partition].next();
            return true;
        } else {
            return false;
        }
    }

    public void processJoin(IFrameWriter writer) throws HyracksDataException {

        if (inputCursor[PROBE_PARTITION].hasNext() && inputCursor[BUILD_PARTITION].hasNext()) {
            // Preload Memory
            inputCursor[BUILD_PARTITION].next();
            inputCursor[PROBE_PARTITION].next();
            // Run Algorithm
            do {
                if (checkToProcessBuildTuple()) {
                    // TuplePrinterUtil.printTuple("  >> processing: ", inputAccessor[RIGHT_PARTITION]);
                    processRemoveOldTuples(PROBE_PARTITION, BUILD_PARTITION, probeKey);
                    addToMemoryAndProcessJoin(PROBE_PARTITION, BUILD_PARTITION, probeKey,
                            iju.checkToRemoveProbeActive(), false, writer);

                } else {
                    // TuplePrinterUtil.printTuple("  >> processing: ", inputAccessor[LEFT_PARTITION]);
                    processRemoveOldTuples(BUILD_PARTITION, PROBE_PARTITION, buildKey);
                    addToMemoryAndProcessJoin(BUILD_PARTITION, PROBE_PARTITION, buildKey,
                            iju.checkToRemoveBuildActive(), true, writer);
                }
            } while (inputCursor[BUILD_PARTITION].hasNext() || inputCursor[PROBE_PARTITION].hasNext());
        }

        resultAppender.write(writer, true);

        activeManager[BUILD_PARTITION].clear();
        activeManager[PROBE_PARTITION].clear();
        runFileStream[BUILD_PARTITION].close();
        runFileStream[PROBE_PARTITION].close();
        runFileStream[BUILD_PARTITION].removeRunFile();
        runFileStream[PROBE_PARTITION].removeRunFile();
    }

    private boolean checkToProcessBuildTuple() {
        if (!inputCursor[BUILD_PARTITION].hasNext()) {
            return true;
        } else if (!inputCursor[PROBE_PARTITION].hasNext()) {
            return false;
        } else {
            long buildStart = IntervalJoinUtil.getIntervalStart(inputCursor[BUILD_PARTITION].getAccessor(),
                    inputCursor[BUILD_PARTITION].getTupleId(), buildKey);
            long probeStart = IntervalJoinUtil.getIntervalStart(inputCursor[PROBE_PARTITION].getAccessor(),
                    inputCursor[PROBE_PARTITION].getTupleId(), probeKey);
            return !(buildStart <= probeStart);
        }

    }

    private void processRemoveOldTuples(int active, int passive, int key) throws HyracksDataException {
        // Remove from passive that can no longer match with active.
        while (activeManager[passive].hasRecords()
                && iju.checkToRemoveInMemory(IntervalJoinUtil.getIntervalStart(inputCursor[active].getAccessor(),
                        inputCursor[active].getTupleId(), key), activeManager[passive].getTopPoint())) {
            activeManager[passive].removeTop();
        }
    }

    private void addToMemoryAndProcessJoin(int active, int passive, int key, boolean removeActive, boolean reversed,
            IFrameWriter writer) throws HyracksDataException {
        // Add to active, end point index and buffer.
        TuplePointer tp = new TuplePointer();
        //if (false) {
        if (activeManager[active].addTuple(inputCursor[active], tp)) {

            //TuplePrinterUtil.printTuple("  added to memory[" + active + "]: ", inputAccessor[active]);

            processTupleJoin(activeManager[passive].getActiveList(), memoryAccessor[passive], inputCursor[active],
                    reversed, writer);
        } else {
            // Spill case
            freezeAndSpill(writer);
            return;
        }
        // Needs to check for another frame.
        inputCursor[active].next();
    }

    private void processTupleJoin(List<TuplePointer> outer, ITupleAccessor outerAccessor, FrameTupleCursor tupleCursor,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        for (TuplePointer outerTp : outer) {
            outerAccessor.reset(outerTp);

            //            TuplePrinterUtil.printTuple("    outer: ", outerAccessor, outerTp.getTupleIndex());
            //            TuplePrinterUtil.printTuple("    inner: ", tupleAccessor);

            if (iju.checkToSaveInResult(outerAccessor, outerTp.getTupleIndex(), tupleCursor.getAccessor(),
                    tupleCursor.getTupleId(), reversed)) { // THis had reversed in it.
                joinSideCount[(reversed) ? 1 : 0]++;
                addToResult(outerAccessor, outerTp.getTupleIndex(), tupleCursor.getAccessor(), tupleCursor.getTupleId(),
                        reversed, writer);
            }
            joinComparisonCount++;
        }
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
        joinResultCount++;
    }

    //Tuple Spilling
    private void processTupleSpill(int active, int passive, int key, boolean removeActive, boolean reversed,
            IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        int count = 0;
        boolean ts = loadTuple(active);
        while (ts && activeManager[passive].hasRecords()) {
            long sweep = activeManager[passive].getTopPoint();
            if (checkToProcessAdd(IntervalJoinUtil.getIntervalStart(inputCursor[active].getAccessor(),
                    inputCursor[active].getTupleId(), key), sweep) || !removeActive) {
                // Add individual tuples.
                processTupleJoin(activeManager[passive].getActiveList(), memoryAccessor[passive], inputCursor[active],
                        reversed, writer);
                if (!runFileStream[active].isReading()) {
                    runFileStream[active].addToRunFile(inputCursor[active]);
                }
                inputCursor[active].next();
                ts = loadTuple(active);
                ++count;
            } else {
                // Remove from active.
                activeManager[passive].removeTop();
            }
        }

        //        if (LOGGER.isLoggable(Level.FINE)) {
        //            LOGGER.fine("Spill for " + count + " tuples");
        //        }

        // Memory is empty and we can start processing the run file.
        if (!activeManager[passive].hasRecords() || !ts) {
            unfreezeAndContinue(active, inputCursor[active]);
            ts = loadTuple(active);
        }
    }

    private void freezeAndSpill(IFrameWriter writer) throws HyracksDataException {
        //        if (LOGGER.isLoggable(Level.FINEST)) {
        //            LOGGER.finest("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
        //                    + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                    + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
        //        }
        //        LOGGER.warning("disk IO: right, " + runFileStream[RIGHT_PARTITION].getReadCount() + " left, "
        //                + runFileStream[LEFT_PARTITION].getReadCount());
        int freezePartition;
        if (bufferManager.getNumTuples(BUILD_PARTITION) > bufferManager.getNumTuples(PROBE_PARTITION)) {
            freezePartition = PROBE_PARTITION;
            rightSpillCount++;
        } else {
            freezePartition = BUILD_PARTITION;
            leftSpillCount++;
        }

        // Mark where to start reading
        if (runFileStream[freezePartition].isReading()) {
            runFilePointer[freezePartition].reset(runFileStream[freezePartition].getReadPointer(),
                    inputCursor[freezePartition].getTupleId());
        } else {
            runFilePointer[freezePartition].reset(0, 0);
            runFileStream[freezePartition].createRunFileWriting();
        }
        // Start writing
        runFileStream[freezePartition].startRunFileWriting();

        if (runFileStream[PROBE_PARTITION].isWriting()) {
            // Right side from disk
            processTupleSpill(PROBE_PARTITION, BUILD_PARTITION, probeKey, iju.checkToRemoveProbeActive(), false,
                    writer);
        } else if (runFileStream[BUILD_PARTITION].isWriting()) {
            // Left side from disk
            processTupleSpill(BUILD_PARTITION, PROBE_PARTITION, buildKey, iju.checkToRemoveBuildActive(), true, writer);
        }
    }

    private void unfreezeAndContinue(int frozenPartition, FrameTupleCursor cursor) throws HyracksDataException {
        int flushPartition = frozenPartition == BUILD_PARTITION ? PROBE_PARTITION : BUILD_PARTITION;
        //                if (LOGGER.isLoggable(Level.FINEST)) {
        //        LOGGER.warning("unfreeze snapshot(" + frozenPartition + "): " + frameCounts[RIGHT_PARTITION] + " right, "
        //                + frameCounts[LEFT_PARTITION] + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION)
        //                + " memory, " + leftSpillCount + " spills, "
        //                + (runFileStream[LEFT_PARTITION].getFileCount() - spillFileCount[LEFT_PARTITION]) + " files, "
        //                + (runFileStream[LEFT_PARTITION].getWriteCount() - spillWriteCount[LEFT_PARTITION]) + " written, "
        //                + (runFileStream[LEFT_PARTITION].getReadCount() - spillReadCount[LEFT_PARTITION]) + " read]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory, " + rightSpillCount + " spills, "
        //                + (runFileStream[RIGHT_PARTITION].getFileCount() - spillFileCount[RIGHT_PARTITION]) + " files, "
        //                + (runFileStream[RIGHT_PARTITION].getWriteCount() - spillWriteCount[RIGHT_PARTITION]) + " written, "
        //                + (runFileStream[RIGHT_PARTITION].getReadCount() - spillReadCount[RIGHT_PARTITION]) + " read].");
        //        spillFileCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getFileCount();
        //        spillReadCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getReadCount();
        //        spillWriteCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getWriteCount();
        //        spillFileCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getFileCount();
        //        spillReadCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getReadCount();
        //        spillWriteCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getWriteCount();
        //                }

        // Finish writing
        runFileStream[frozenPartition].flushRunFile();

        // Clear memory
        flushMemory(flushPartition);
        if ((BUILD_PARTITION == frozenPartition && !runFileStream[BUILD_PARTITION].isReading())
                || (PROBE_PARTITION == frozenPartition && !runFileStream[PROBE_PARTITION].isReading())) {
            streamIndex[frozenPartition] = cursor.getTupleId();
        }

        // Start reading
        runFileStream[frozenPartition].startReadingRunFile(cursor, runFilePointer[frozenPartition].getFileOffset());
        cursor.resetPosition(runFilePointer[frozenPartition].getTupleIndex());
        runFilePointer[frozenPartition].reset(-1, -1);

        //        System.err.println("unfreeze snapshot(" + frozenPartition + "): " + frameCounts[RIGHT_PARTITION] + " right, "
        //                + frameCounts[LEFT_PARTITION] + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION)
        //                + " memory, " + leftSpillCount + " spills, "
        //                + (runFileStream[LEFT_PARTITION].getFileCount() - spillFileCount[LEFT_PARTITION]) + " files, "
        //                + (runFileStream[LEFT_PARTITION].getWriteCount() - spillWriteCount[LEFT_PARTITION]) + " written, "
        //                + (runFileStream[LEFT_PARTITION].getReadCount() - spillReadCount[LEFT_PARTITION]) + " read]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory, " + rightSpillCount + " spills, "
        //                + (runFileStream[RIGHT_PARTITION].getFileCount() - spillFileCount[RIGHT_PARTITION]) + " files, "
        //                + (runFileStream[RIGHT_PARTITION].getWriteCount() - spillWriteCount[RIGHT_PARTITION]) + " written, "
        //                + (runFileStream[RIGHT_PARTITION].getReadCount() - spillReadCount[RIGHT_PARTITION]) + " read].");
        spillFileCount[BUILD_PARTITION] = runFileStream[BUILD_PARTITION].getFileCount();
        spillReadCount[BUILD_PARTITION] = runFileStream[BUILD_PARTITION].getReadCount();
        spillWriteCount[BUILD_PARTITION] = runFileStream[BUILD_PARTITION].getWriteCount();
        spillFileCount[PROBE_PARTITION] = runFileStream[PROBE_PARTITION].getFileCount();
        spillReadCount[PROBE_PARTITION] = runFileStream[PROBE_PARTITION].getReadCount();
        spillWriteCount[PROBE_PARTITION] = runFileStream[PROBE_PARTITION].getWriteCount();

        //if (LOGGER.isLoggable(Level.FINE)) {
        //    LOGGER.fine("Unfreezing (" + frozenPartition + ").");
        //}
    }

    private void flushMemory(int partition) throws HyracksDataException {
        activeManager[partition].clear();
    }

    private boolean checkToProcessAdd(long startMemory, long endMemory) {
        return startMemory <= endMemory;
    }

}
