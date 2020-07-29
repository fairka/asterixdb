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

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int PROBE_PARTITION = 0;
    protected static final int BUILD_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final ITupleAccessor[] inputAccessor;
    protected final IntervalMergeStatus status;

    protected long[] frameCounts = { 0, 0 };
    protected long[] tupleCounts = { 0, 0 };

    public IntervalMergeJoiner(IHyracksTaskContext ctx, int memorySize, IntervalMergeStatus status,
            IIntervalJoinChecker mjc, int leftKeys, int rightKeys, RecordDescriptor leftRd, RecordDescriptor rightRd)
            throws HyracksDataException {
        this.mjc = mjc;
        this.status = status;

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[PROBE_PARTITION] = new TupleAccessor(leftRd);
        inputAccessor[BUILD_PARTITION] = new TupleAccessor(rightRd);

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);

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
        runFileStream = new RunFileStream(ctx, "ismj-left", status.branch[BUILD_PARTITION]);
        runFilePointer = new RunFilePointer();
        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();

        memoryTuple = new IntervalSideTuple(mjc, memoryAccessor, rightKeys);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[PROBE_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[PROBE_PARTITION], leftKeys);
        inputTuple[BUILD_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[BUILD_PARTITION], rightKeys);
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
    }

    private void flushMemory() throws HyracksDataException {
        memoryBuffer.clear();
        bufferManager.reset();
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    private TupleStatus loadProbeTuple() throws HyracksDataException {
        TupleStatus loaded = loadMemoryTuple(BUILD_PARTITION);
        return loaded;
    }

    private TupleStatus loadBuildTuple() throws HyracksDataException {
        TupleStatus loaded;
        loaded = loadSpilledTuple(PROBE_PARTITION);
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

    public void initializeProbeFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor[PROBE_PARTITION].reset(buffer);
        inputAccessor[PROBE_PARTITION].next();

    }

    public void processProbeFrame(IFrameWriter writer) throws HyracksDataException {
        TupleStatus buildTs = loadBuildTuple();
        TupleStatus probeTs = loadProbeTuple();
        while (buildTs.isLoaded() && (probeTs.isLoaded() || memoryHasTuples())) {
            if (probeTs.isLoaded()) {
                // Right side from stream
                processProbeTuple(writer);
                probeTs = loadProbeTuple();
            } else {
                // Left side from stream
                processBuildTuple(writer);
                buildTs = loadBuildTuple();
            }
        }
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {
        if (runFileStream.isWriting()) {
            unfreezeAndContinue(inputAccessor[PROBE_PARTITION]);
        }
        processProbeFrame(writer);
        resultAppender.write(writer, true);
        runFileStream.removeRunFile();
    }

    private TupleStatus processBuildTupleSpill(IFrameWriter writer) throws HyracksDataException {

        processBuildTuple(writer);

        // Memory is empty and we can start processing the run file.
        if (!memoryHasTuples() && runFileStream.isWriting()) {
            unfreezeAndContinue(inputAccessor[PROBE_PARTITION]);
        }
        return loadBuildTuple();
    }

    private void processBuildTuple(IFrameWriter writer) throws HyracksDataException {
        // Check against memory (right)
        if (memoryHasTuples()) {
            inputTuple[PROBE_PARTITION].loadTuple();
            Iterator<TuplePointer> memoryIterator = memoryBuffer.iterator();
            while (memoryIterator.hasNext()) {
                TuplePointer tp = memoryIterator.next();
                memoryTuple.setTuple(tp);
                if (inputTuple[PROBE_PARTITION].removeFromMemory(memoryTuple)) {
                    // remove from memory
                    bufferManager.deleteTuple(tp);
                    memoryIterator.remove();
                    continue;
                } else if (inputTuple[PROBE_PARTITION].checkForEarlyExit(memoryTuple)) {
                    // No more possible comparisons
                    break;
                } else if (inputTuple[PROBE_PARTITION].compareJoin(memoryTuple)) {
                    // add to result
                    addToResult(inputAccessor[PROBE_PARTITION], inputAccessor[PROBE_PARTITION].getTupleId(),
                            memoryAccessor, tp.getTupleIndex(), writer);
                }
            }
        }
        inputAccessor[PROBE_PARTITION].next();
    }

    private void processProbeTuple(IFrameWriter writer) throws HyracksDataException {
        // append to memory
        if (mjc.checkToSaveInMemory(inputAccessor[PROBE_PARTITION], inputAccessor[BUILD_PARTITION])) {
            // Must be a separate if statement.
            if (!addToMemory(inputAccessor[BUILD_PARTITION])) {
                // go to log saving state
                freezeAndSpill();
                TupleStatus buildTs = loadBuildTuple();
                while (buildTs.isLoaded() && memoryHasTuples()) {
                    // Left side from stream
                    processBuildTuple(writer);
                    buildTs = loadBuildTuple();
                }
                unfreezeAndContinue(inputAccessor[PROBE_PARTITION]);
                return;
            }
        }
        inputAccessor[BUILD_PARTITION].next();
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputAccessor[BUILD_PARTITION].getTupleCount(); x++) {
            runFileStream.addToRunFile(inputAccessor[BUILD_PARTITION], x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(inputAccessor[BUILD_PARTITION]);
    }

    private void freezeAndSpill() throws HyracksDataException {
        // Mark where to start reading
        runFilePointer.reset(runFileStream.getReadPointer(), inputAccessor[PROBE_PARTITION].getTupleId());

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "Memory is full. Freezing the right branch. (memory tuples: " + bufferManager.getNumTuples() + ")");
        }
    }

    private void continueStream(ITupleAccessor accessor) throws HyracksDataException {
        // Stop reading.
        runFileStream.closeRunFileReading();
        if (runFilePointer.getFileOffset() < 0) {
            // Remove file if not needed.
            runFileStream.close();
        }

        // Continue on stream
        accessor.reset(inputBuffer[PROBE_PARTITION].getBuffer());
        accessor.setTupleId(leftStreamIndex);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Continue with left stream.");
        }
    }

    private void unfreezeAndContinue(ITupleAccessor accessor) throws HyracksDataException {
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
        if (inputAccessor[BUILD_PARTITION] != null && !inputAccessor[BUILD_PARTITION].exists()
                && status.branch[BUILD_PARTITION].getStatus() == IntervalMergeBranchStatus.Stage.CLOSED) {
            status.branch[BUILD_PARTITION].noMore();
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
