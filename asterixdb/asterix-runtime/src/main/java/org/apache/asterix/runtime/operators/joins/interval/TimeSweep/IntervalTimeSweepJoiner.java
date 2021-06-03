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
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.interval.TimeSweep.memory.VPartitionDeletableTupleBufferManager;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
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

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    private final IIntervalJoinUtil iju;
    private final int buildKey;
    private final int probeKey;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleCursor[] inputCursor;
    protected final FrameTupleAppender resultAppender;
    private final VPartitionDeletableTupleBufferManager bufferManager;
    private final ITupleAccessor[] memoryAccessor;
    private final RunFilePointer[] runFilePointer;

    private final int[] streamIndex;

    private final ActiveSweepManager[] activeManager;
    private final byte point;

    private final RunFileStream[] runFileStream;

    public IntervalTimeSweepJoiner(IHyracksTaskContext ctx, int memorySize, IIntervalJoinUtil iju, int buildKey,
            int probeKey, RecordDescriptor buildRd, RecordDescriptor probeRd) throws HyracksDataException {
        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "IntervalMergeJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }

        this.iju = iju;
        this.buildKey = buildKey;
        this.probeKey = probeKey;

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        inputCursor[PROBE_PARTITION] = new FrameTupleCursor(probeRd);

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[BUILD_PARTITION] = buildRd;
        recordDescriptors[PROBE_PARTITION] = probeRd;

        //Two frames are used for the runfile stream, and one frame for each input (2 outputs)
        bufferManager =
                new VPartitionDeletableTupleBufferManager(ctx, VPartitionDeletableTupleBufferManager.NO_CONSTRAIN,
                        JOIN_PARTITIONS, memorySize * ctx.getInitialFrameSize(), recordDescriptors);
        memoryAccessor = new ITupleAccessor[JOIN_PARTITIONS];
        memoryAccessor[PROBE_PARTITION] = bufferManager.getTupleAccessor(probeRd);
        memoryAccessor[BUILD_PARTITION] = bufferManager.getTupleAccessor(buildRd);

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[BUILD_PARTITION] = new RunFileStream(ctx, "imj-build");
        runFileStream[PROBE_PARTITION] = new RunFileStream(ctx, "itsj-probe");
        runFileStream[BUILD_PARTITION].createRunFileWriting();
        runFileStream[BUILD_PARTITION].startRunFileWriting();
        runFileStream[PROBE_PARTITION].createRunFileWriting();
        runFileStream[PROBE_PARTITION].startRunFileWriting();

        this.point = EndPointIndexItem.END_POINT;
        activeManager = new ActiveSweepManager[JOIN_PARTITIONS];
        activeManager[BUILD_PARTITION] = new ActiveSweepManager(bufferManager, buildKey, BUILD_PARTITION,
                EndPointIndexItem.EndPointAscComparator, point);
        activeManager[PROBE_PARTITION] = new ActiveSweepManager(bufferManager, probeKey, PROBE_PARTITION,
                EndPointIndexItem.EndPointAscComparator, point);

        runFilePointer = new RunFilePointer[JOIN_PARTITIONS];
        runFilePointer[BUILD_PARTITION] = new RunFilePointer();
        runFilePointer[PROBE_PARTITION] = new RunFilePointer();

        streamIndex = new int[JOIN_PARTITIONS];
        streamIndex[BUILD_PARTITION] = -1;
        streamIndex[PROBE_PARTITION] = -1;

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputCursor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[BUILD_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream[BUILD_PARTITION].addToRunFile(inputCursor[BUILD_PARTITION].getAccessor(), x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
        runFileStream[BUILD_PARTITION].flushRunFile();
        runFileStream[BUILD_PARTITION].startReadingRunFile(inputCursor[BUILD_PARTITION]);
    }

    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        inputCursor[PROBE_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[PROBE_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream[PROBE_PARTITION].addToRunFile(inputCursor[PROBE_PARTITION].getAccessor(), x);
        }
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {

        runFileStream[PROBE_PARTITION].flushRunFile();
        runFileStream[PROBE_PARTITION].startReadingRunFile(inputCursor[PROBE_PARTITION]);

        processJoin(writer);

        runFileStream[PROBE_PARTITION].close();
        runFileStream[BUILD_PARTITION].close();
    }

    private boolean hasNext(int partition) throws HyracksDataException {
        if (inputCursor[partition].hasNext()) {
            // Must keep condition in a separate `if` due to actions applied in loadNextBuffer.
            return true;
        } else {
            return runFileStream[partition].loadNextBuffer(inputCursor[partition]);
        }
    }

    public void processJoin(IFrameWriter writer) throws HyracksDataException {

        // Initialize Variables
        boolean continueBuild = true;
        boolean continueProbe = true;
        boolean choseProbePath;

        if (hasNext(PROBE_PARTITION) && hasNext(BUILD_PARTITION)) {
            // Preload Memory
            inputCursor[BUILD_PARTITION].next();
            inputCursor[PROBE_PARTITION].next();
            // Centralize Has next
            // Run Algorithm
            do {
                // Choose Correct Path
                if (!continueBuild) {
                    choseProbePath = true;
                } else if (!continueProbe) {
                    choseProbePath = false;
                } else {
                    choseProbePath = iju.choosePath(inputCursor[BUILD_PARTITION].getAccessor(),
                            inputCursor[BUILD_PARTITION].getTupleId(), inputCursor[PROBE_PARTITION].getAccessor(),
                            inputCursor[PROBE_PARTITION].getTupleId());
                }
                // Process the correct side based on chosen path.
                if (choseProbePath) {
                    addToMemoryAndProcessJoin(PROBE_PARTITION, BUILD_PARTITION, false, writer);
                    // Needs to check for another frame
                    continueProbe = hasNext(PROBE_PARTITION);
                    if (continueProbe) {
                        inputCursor[PROBE_PARTITION].next();
                    }

                } else {
                    addToMemoryAndProcessJoin(BUILD_PARTITION, PROBE_PARTITION, true, writer);
                    // Needs to check for another frame
                    continueBuild = hasNext(BUILD_PARTITION);
                    if (continueBuild) {
                        inputCursor[BUILD_PARTITION].next();
                    }
                }
                // Based on continue, not has next.
            } while (continueProbe || continueBuild);
        }

        resultAppender.write(writer, true);

        activeManager[BUILD_PARTITION].clear();
        activeManager[PROBE_PARTITION].clear();
        runFileStream[BUILD_PARTITION].close();
        runFileStream[PROBE_PARTITION].close();
        runFileStream[BUILD_PARTITION].removeRunFile();
        runFileStream[PROBE_PARTITION].removeRunFile();
    }

    private void addToMemoryAndProcessJoin(int memoryPartition, int streamPartition, boolean reversed,
            IFrameWriter writer) throws HyracksDataException {
        //Add to active, end point index and buffer.
        TuplePointer tp = new TuplePointer();
        if (activeManager[memoryPartition].addTuple(inputCursor[memoryPartition], tp)) {
            processTupleJoin(activeManager[streamPartition].getActiveList(), memoryAccessor[streamPartition],
                    inputCursor[memoryPartition], reversed, writer);
            return;
        }
        freezeAndSpill(writer);
        addToMemoryAndProcessJoin(memoryPartition, streamPartition, reversed, writer);
    }

    private void freezeAndSpill(IFrameWriter writer) throws HyracksDataException {

        int streamPartition;
        int memoryPartition;
        boolean reversed = false;
        if (bufferManager.getNumTuples(BUILD_PARTITION) > bufferManager.getNumTuples(PROBE_PARTITION)) {
            memoryPartition = BUILD_PARTITION;
            streamPartition = PROBE_PARTITION;
        } else {
            memoryPartition = PROBE_PARTITION;
            streamPartition = BUILD_PARTITION;
            reversed = true;
        }
        runFilePointer[streamPartition].reset(runFileStream[streamPartition].getReadPointer(),
                inputCursor[streamPartition].getTupleId());

        processTupleSpill(memoryPartition, streamPartition, reversed, writer);

        // Clear memory
        activeManager[memoryPartition].clear();

        // Reset Position of Run File and cursor on frozen side.
        runFileStream[streamPartition].startReadingRunFile(inputCursor[streamPartition],
                runFilePointer[streamPartition].getFileOffset());
        inputCursor[streamPartition].resetPosition(runFilePointer[streamPartition].getTupleIndex());
        runFilePointer[streamPartition].reset(-1, -1);
    }

    private void processTupleSpill(int memoryPartition, int streamPartition, boolean reversed, IFrameWriter writer)
            throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        boolean continueStream = true;
        while (continueStream) {
            // Add individual tuples.
            processTupleJoin(activeManager[memoryPartition].getActiveList(), memoryAccessor[memoryPartition],
                    inputCursor[streamPartition], reversed, writer);
            continueStream = hasNext(streamPartition);
            if (continueStream) {
                inputCursor[streamPartition].next();
            }
        }
    }

    private void processTupleJoin(List<TuplePointer> outer, ITupleAccessor outerAccessor, FrameTupleCursor tupleCursor,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        for (TuplePointer outerTp : outer) {
            outerAccessor.reset(outerTp);
            if (iju.checkToSaveInResult(outerAccessor, outerTp.getTupleIndex(), tupleCursor.getAccessor(),
                    tupleCursor.getTupleId(), reversed)) {
                addToResult(outerAccessor, outerTp.getTupleIndex(), tupleCursor.getAccessor(), tupleCursor.getTupleId(),
                        reversed, writer);
            }
        }
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
    }

}
