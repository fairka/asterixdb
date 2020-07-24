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
package org.apache.asterix.runtime.operators.joins.intervalmergejoin;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.TuplePrinterUtil;
import org.apache.asterix.runtime.operators.joins.Utils.IIntervalJoinChecker;
import org.apache.asterix.runtime.operators.joins.Utils.IntervalSideTuple;
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
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 */
public class IntervalMergeJoiner {

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

    private static final Logger LOGGER = Logger.getLogger(IntervalMergeJoiner.class.getName());

    private final IDeallocatableFramePool framePool;
    private final IDeletableTupleBufferManager bufferManager;
    private final ITupleAccessor memoryAccessor;
    private final LinkedList<TuplePointer> memoryBuffer = new LinkedList<>();

    private int leftStreamIndex;
    private final RunFileStream runFileStream;
    private final RunFilePointer runFilePointer;

    private IntervalSideTuple memoryTuple;
    private IntervalSideTuple[] inputTuple;

    private final IIntervalJoinChecker mjc;

    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private long spillFileCount = 0;
    private long spillWriteCount = 0;
    private long spillReadCount = 0;
    private long spillCount = 0;

    private final int partition;
    private final int memorySize;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final ITupleAccessor[] inputAccessor;
    protected final IntervalMergeStatus status;

    private final IntervalMergeJoinLocks locks;
    protected long[] frameCounts = { 0, 0 };
    protected long[] tupleCounts = { 0, 0 };

    private final boolean DEBUG = true;

    public IntervalMergeJoiner(IHyracksTaskContext ctx, int memorySize, int partition, IntervalMergeStatus status,
            IntervalMergeJoinLocks locks, IIntervalJoinChecker mjc, int[] leftKeys, int[] rightKeys,
            RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        this.mjc = mjc;
        this.partition = partition;
        this.memorySize = memorySize;
        this.status = status;
        this.locks = locks;

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftRd);
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightRd);

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[RIGHT_PARTITION] = new VSizeFrame(ctx);

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        // Memory (right buffer)
        if (memorySize < 1) {
            throw new HyracksDataException(
                    "MergeJoiner does not have enough memory (needs > 0, got " + memorySize + ").");
        }

        framePool = new DeallocatableFramePool(ctx, (memorySize) * ctx.getInitialFrameSize());
        bufferManager = new VariableDeletableTupleMemoryManager(framePool, rightRd);
        memoryAccessor = bufferManager.createTupleAccessor();

        // Run File and frame cache (left buffer)
        leftStreamIndex = TupleAccessor.UNSET;
        runFileStream = new RunFileStream(ctx, "ismj-left", status.branch[LEFT_PARTITION]);
        runFilePointer = new RunFilePointer();

        memoryTuple = new IntervalSideTuple(mjc, memoryAccessor, rightKeys[0]);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[LEFT_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[LEFT_PARTITION], leftKeys[0]);
        inputTuple[RIGHT_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[RIGHT_PARTITION], rightKeys[0]);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning(
                    "MergeJoiner has started partition " + partition + " with " + memorySize + " frames of memory.");
        }
    }

    private boolean addToMemory(ITupleAccessor accessor) throws HyracksDataException {
        TuplePointer tp = new TuplePointer();
        if (bufferManager.insertTuple(accessor, accessor.getTupleId(), tp)) {
            memoryBuffer.add(tp);
            return true;
        }
        return false;
    }

    private void addToResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessorLeft, leftTupleIndex, accessorRight,
                rightTupleIndex);
        joinResultCount++;
    }

    private void flushMemory() throws HyracksDataException {
        memoryBuffer.clear();
        bufferManager.reset();
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadRightTuple() throws HyracksDataException {
        TupleStatus loaded = loadMemoryTuple(RIGHT_PARTITION);
        if (loaded == TupleStatus.UNKNOWN) {
            loaded = pauseAndLoadRightTuple();
        }
        return loaded;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadLeftTuple() throws HyracksDataException {
        TupleStatus loaded;
        if (runFileStream.isReading()) {
            loaded = loadSpilledTuple(LEFT_PARTITION);
            if (loaded.isEmpty()) {
                if (runFileStream.isWriting() && !status.branch[LEFT_PARTITION].hasMore()) {
                    unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
                } else {
                    continueStream(inputAccessor[LEFT_PARTITION]);
                }
                loaded = loadLeftTuple();
            }
        } else {
            loaded = loadMemoryTuple(LEFT_PARTITION);
        }
        return loaded;
    }

    private TupleStatus loadSpilledTuple(int partition) throws HyracksDataException {
        if (!inputAccessor[partition].exists()) {
            // Must keep condition in a separate if due to actions applied in loadNextBuffer.
            if (!runFileStream.loadNextBuffer(inputAccessor[partition])) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    public void processLeftFrame(IFrameWriter writer) throws HyracksDataException {
        TupleStatus leftTs = loadLeftTuple();
        TupleStatus rightTs = loadRightTuple();
        while (leftTs.isLoaded() && (status.branch[RIGHT_PARTITION].hasMore() || memoryHasTuples())) {
            if (runFileStream.isWriting()) {
                // Left side from disk
                leftTs = processLeftTupleSpill(writer);
            } else if (rightTs.isLoaded()
                    && mjc.checkToLoadNextRightTuple(inputAccessor[LEFT_PARTITION], inputAccessor[RIGHT_PARTITION])) {
                // Right side from stream
                processRightTuple();
                rightTs = loadRightTuple();
            } else {
                // Left side from stream
                processLeftTuple(writer);
                leftTs = loadLeftTuple();
            }
        }
    }

    public void processLeftClose(IFrameWriter writer) throws HyracksDataException {
        if (runFileStream.isWriting()) {
            unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
        }
        processLeftFrame(writer);
        resultAppender.write(writer, true);

        System.out.println("MergeJoiner Statistics Log ––" + " Partition: " + partition + ", Memory Size: " + memorySize
                + ", Number of Results: " + joinResultCount + ", JoinComparisonCount: " + joinComparisonCount
                + ", Spill Count: " + spillCount + ", Frames Written: " + runFileStream.getWriteCount()
                + ", Read Count: " + runFileStream.getReadCount());
        //        if (LOGGER.isLoggable(Level.WARNING)) {
        //            LOGGER.warning("MergeJoiner Statistics Log ––" + " Partition: " + partition + ", Memory Size: " + memorySize
        //                    + ", Number of Results: " + joinResultCount + ", JoinComparisonCount: " + joinComparisonCount
        //                    + ", Spill Count: " + spillCount + ", Frames Written: " + runFileStream.getWriteCount()
        //                    + ", Read Count: " + runFileStream.getReadCount());
        //        }
        runFileStream.removeRunFile();
    }

    private TupleStatus processLeftTupleSpill(IFrameWriter writer) throws HyracksDataException {
        if (!runFileStream.isReading()) {
            runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);
        }

        processLeftTuple(writer);

        // Memory is empty and we can start processing the run file.
        if (!memoryHasTuples() && runFileStream.isWriting()) {
            unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
        }
        return loadLeftTuple();
    }

    private void processLeftTuple(IFrameWriter writer) throws HyracksDataException {
        // Check against memory (right)
        if (memoryHasTuples()) {
            inputTuple[LEFT_PARTITION].loadTuple();
            Iterator<TuplePointer> memoryIterator = memoryBuffer.iterator();
            while (memoryIterator.hasNext()) {
                TuplePointer tp = memoryIterator.next();
                memoryTuple.setTuple(tp);
                if (DEBUG) {
                    String string = TuplePrinterUtil.printTuple("    stream: ", inputAccessor[LEFT_PARTITION]);
                    String string2 = TuplePrinterUtil.printTuple("    memory: ", memoryTuple.getAccessor(),
                            memoryTuple.getTupleIndex());
                    System.err.println("MERGE test from stream: " + tp + "\n" + string + "\n" + string2);
                }
                if (inputTuple[LEFT_PARTITION].removeFromMemory(memoryTuple)) {
                    if (DEBUG) {
                        System.err.println("REMOVE from memory: " + tp);
                        String string = TuplePrinterUtil.printTuple("    memory: ", memoryTuple.getAccessor(),
                                memoryTuple.getTupleIndex());
                        System.err.println("REMOVE from memory: " + tp + "\n" + string);
                    }
                    // remove from memory
                    bufferManager.deleteTuple(tp);
                    memoryIterator.remove();
                    continue;
                } else if (inputTuple[LEFT_PARTITION].checkForEarlyExit(memoryTuple)) {
                    // No more possible comparisons
                    break;
                } else if (inputTuple[LEFT_PARTITION].compareJoin(memoryTuple)) {
                    // add to result
                    addToResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                            memoryAccessor, tp.getTupleIndex(), writer);
                }
                joinComparisonCount++;
            }
        }
        inputAccessor[LEFT_PARTITION].next();
    }

    private void processRightTuple() throws HyracksDataException {
        // append to memory
        if (mjc.checkToSaveInMemory(inputAccessor[LEFT_PARTITION], inputAccessor[RIGHT_PARTITION])) {
            // Must be a separate if statement.
            if (!addToMemory(inputAccessor[RIGHT_PARTITION])) {
                // go to log saving state
                freezeAndSpill();
                return;
            }
            if (DEBUG) {
                String string = TuplePrinterUtil.printTuple("    memory: ", inputAccessor[RIGHT_PARTITION]);
                System.err.println("ADD to memory: \n" + string);
            }
        }
        inputAccessor[RIGHT_PARTITION].next();
    }

    private void freezeAndSpill() throws HyracksDataException {
        //        if (LOGGER.isLoggable(Level.WARNING)) {
        if (DEBUG) {
            System.err.println("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, "
                    + frameCounts[LEFT_PARTITION] + " left, " + joinComparisonCount + " comparisons, " + joinResultCount
                    + " results, [" + bufferManager.getNumTuples() + " tuples memory].");
        }
        // Mark where to start reading
        if (runFileStream.isReading()) {
            runFilePointer.reset(runFileStream.getReadPointer(), inputAccessor[LEFT_PARTITION].getTupleId());
        } else {
            runFilePointer.reset(0, 0);
            runFileStream.createRunFileWriting();
        }
        // Start writing
        runFileStream.startRunFileWriting();

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "Memory is full. Freezing the right branch. (memory tuples: " + bufferManager.getNumTuples() + ")");
        }
        spillCount++;
    }

    private void continueStream(ITupleAccessor accessor) throws HyracksDataException {
        // Stop reading.
        runFileStream.closeRunFileReading();
        if (runFilePointer.getFileOffset() < 0) {
            // Remove file if not needed.
            runFileStream.close();
        }

        // Continue on stream
        accessor.reset(inputBuffer[LEFT_PARTITION].getBuffer());
        accessor.setTupleId(leftStreamIndex);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Continue with left stream.");
        }
    }

    private void unfreezeAndContinue(ITupleAccessor accessor) throws HyracksDataException {
        //        if (LOGGER.isLoggable(Level.WARNING)) {
        if (DEBUG) {
            System.err.println("snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
                    + " left, " + joinComparisonCount + " comparisons, " + joinResultCount + " results, ["
                    + bufferManager.getNumTuples() + " tuples memory, " + spillCount + " spills, "
                    + (runFileStream.getFileCount() - spillFileCount) + " files, "
                    + (runFileStream.getWriteCount() - spillWriteCount) + " written, "
                    + (runFileStream.getReadCount() - spillReadCount) + " read].");
            spillFileCount = runFileStream.getFileCount();
            spillReadCount = runFileStream.getReadCount();
            spillWriteCount = runFileStream.getWriteCount();
        }

        // Finish writing
        runFileStream.flushRunFile();

        // Clear memory
        flushMemory();
        if (!runFileStream.isReading()) {
            leftStreamIndex = accessor.getTupleId();
        }

        // Start reading
        runFileStream.startReadingRunFile(accessor, runFilePointer.getFileOffset());
        accessor.setTupleId(runFilePointer.getTupleIndex());
        runFilePointer.reset(-1, -1);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Unfreezing right partition.");
        }
    }

    protected TupleStatus loadMemoryTuple(int branch) {
        TupleStatus loaded;
        if (inputAccessor[branch] != null && inputAccessor[branch].exists()) {
            // Still processing frame.
            int test = inputAccessor[branch].getTupleCount();
            loaded = TupleStatus.LOADED;
        } else if (status.branch[branch].hasMore()) {
            loaded = TupleStatus.UNKNOWN;
        } else {
            // No more frames or tuples to process.
            loaded = TupleStatus.EMPTY;
        }
        return loaded;
    }

    protected TupleStatus pauseAndLoadRightTuple() {
        status.continueRightLoad = true;
        locks.getRight(partition).signal();
        try {
            while (status.continueRightLoad && status.branch[RIGHT_PARTITION].getStatus()
                    .isEqualOrBefore(IntervalMergeBranchStatus.Stage.DATA_PROCESSING)) {
                locks.getLeft(partition).await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (inputAccessor[RIGHT_PARTITION] != null && !inputAccessor[RIGHT_PARTITION].exists()
                && status.branch[RIGHT_PARTITION].getStatus() == IntervalMergeBranchStatus.Stage.CLOSED) {
            status.branch[RIGHT_PARTITION].noMore();
            return TupleStatus.EMPTY;
        }
        return TupleStatus.LOADED;
    }

    public void setFrame(int branch, ByteBuffer buffer) throws HyracksDataException {
        inputBuffer[branch].getBuffer().clear();
        if (inputBuffer[branch].getFrameSize() < buffer.capacity()) {
            inputBuffer[branch].resize(buffer.capacity());
        }
        inputBuffer[branch].getBuffer().put(buffer.array(), 0, buffer.capacity());
        inputAccessor[branch].reset(inputBuffer[branch].getBuffer());
        inputAccessor[branch].next();
        frameCounts[branch]++;
        tupleCounts[branch] += inputAccessor[branch].getTupleCount();
    }
}
