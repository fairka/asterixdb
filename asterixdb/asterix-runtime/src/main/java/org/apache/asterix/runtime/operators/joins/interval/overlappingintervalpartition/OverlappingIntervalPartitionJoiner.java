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
package org.apache.asterix.runtime.operators.joins.interval.overlappingintervalpartition;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class OverlappingIntervalPartitionJoiner extends AbstractStreamJoiner {

    private static final Logger LOGGER = Logger.getLogger(OverlappingIntervalPartitionJoiner.class.getName());

    private final RunFileWriter probeRunFileWriter;
    private int probeRunFilePid = -1;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final int k;
    private final int numOfPartitions;
    private long[] buildPartitionSizes;
    private long[] probePartitionSizes;
    private final TreeMap<RunFilePointer, Integer> probeRunFilePointers;

    private final VPartitionTupleBufferManager buildBufferManager;
    private final TuplePointer tempPtr = new TuplePointer();
    private final List<Integer> buildInMemoryPartitions;
    private final FrameTupleAccessor accessorBuild;
    private BufferInfo bufferInfo;

    private long spillWriteCount = 0;
    private long spillReadCount = 0;
    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private final IIntervalJoinUtil imjc;
    private final FrameTupleAccessor accessorProbe;
    private final IFrame reloadBuffer;
    private boolean moreBuildProcessing = true;
    private final List<IFrameBufferManager> fbms = new ArrayList<>();

    private final int partition;
    private final int memorySize;

    public OverlappingIntervalPartitionJoiner(IHyracksTaskContext ctx, int memorySize, int partition, int k,
            IIntervalJoinUtil imjc, IConsumerFrame leftCF, IConsumerFrame rightCF, ITuplePartitionComputer buildHpc,
            ITuplePartitionComputer probeHpc) throws HyracksDataException {
        super(ctx, partition, leftCF, rightCF);
        this.partition = partition;
        this.memorySize = memorySize;

        bufferInfo = new BufferInfo(null, -1, -1);

        this.accessorProbe = new FrameTupleAccessor(leftCF.getRecordDescriptor());
        reloadBuffer = new VSizeFrame(ctx);

        this.numOfPartitions = OverlappingIntervalPartitionUtil.getMaxPartitions(k);
        this.imjc = imjc;
        buildPartitionSizes = new long[numOfPartitions];
        probePartitionSizes = new long[numOfPartitions];

        // TODO fix available memory size
        buildBufferManager = new VPartitionTupleBufferManager(ctx, VPartitionTupleBufferManager.NO_CONSTRAIN,
                numOfPartitions, memorySize * ctx.getInitialFrameSize());

        this.k = k;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;

        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("OverlappingIntervalPartitionJoiner");
        probeRunFileWriter = new RunFileWriter(file, ctx.getIoManager());
        probeRunFileWriter.open();

        probeRunFilePointers = new TreeMap<>(RunFilePointer.ASC);
        buildInMemoryPartitions = new LinkedList<>();

        this.accessorBuild = new FrameTupleAccessor(rightCF.getRecordDescriptor());
    }

    @Override
    public void processJoin(IFrameWriter writer) throws HyracksDataException {
        while (getNextFrame(LEFT_PARTITION)) {
            while (inputAccessor[LEFT_PARTITION].exists()) {
                int pid = probeHpc.partition(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                        k);

                if (probeRunFilePid != pid) {
                    // Log new partition locations.
                    RunFilePointer rfp = new RunFilePointer(probeRunFileWriter.getFileSize(),
                            inputAccessor[LEFT_PARTITION].getTupleId());
                    probeRunFilePointers.put(rfp, pid);
                    probeRunFilePid = pid;
                }
                inputAccessor[LEFT_PARTITION].next();
                probePartitionSizes[pid]++;
            }
            inputBuffer[LEFT_PARTITION].getBuffer().rewind();
            probeRunFileWriter.nextFrame(inputBuffer[LEFT_PARTITION].getBuffer());
            spillWriteCount++;
        }

        joinLoopOnMemory(writer);

        // Flush result.
        resultAppender.write(writer, true);
        if (LOGGER.isLoggable(Level.WARNING)) {
            long ioCost = spillWriteCount + spillReadCount;
            LOGGER.warning(",OverlappingIntervalPartitionJoiner Statistics Log," + partition + ",partition,"
                    + memorySize + ",memory," + joinResultCount + ",results," + joinComparisonCount + ",CPU," + ioCost
                    + ",IO," + k + ",k," + spillWriteCount + ",frames_written," + spillReadCount + ",frames_read");
        }
        printPartitionStatus();
        long ioCost = spillWriteCount + spillReadCount;
        System.err.println(",OverlappingIntervalPartitionJoiner Statistics Log," + partition + ",partition,"
                + memorySize + ",memory," + joinResultCount + ",results," + joinComparisonCount + ",CPU," + ioCost
                + ",IO," + k + ",k," + spillWriteCount + ",frames_written," + spillReadCount + ",frames_read");
        probeRunFileWriter.close();
    }

    private void printPartitionStatus() {
        System.err.println("k=" + k + " numOfPartitions:" + numOfPartitions);
        System.err.println("left:" + frameCounts[LEFT_PARTITION] + " right:" + frameCounts[RIGHT_PARTITION]);
        System.err.println("build[");
        int count = 0;
        for (int i = 0; i < buildPartitionSizes.length; i++) {
            if (buildPartitionSizes[i] > 0) {
                System.err.println("  (" + OverlappingIntervalPartitionUtil.getIntervalPartition(i, k) + ") "
                        + buildPartitionSizes[i] + " ");
                count++;
            }
        }
        System.err.println("]");
        System.err.println("Used Partitions:" + count);

        System.err.println("probe[");
        count = 0;
        for (int i = 0; i < probePartitionSizes.length; i++) {
            if (probePartitionSizes[i] > 0) {
                System.err.println("  (" + OverlappingIntervalPartitionUtil.getIntervalPartition(i, k) + ") "
                        + probePartitionSizes[i] + " ");
                count++;
            }
        }
        System.err.println("]");
        System.err.println("Used Partitions:" + count);
    }

    private void joinLoopOnMemory(IFrameWriter writer) throws HyracksDataException {
        RunFileReader pReader = probeRunFileWriter.createDeleteOnCloseReader();
        pReader.open();
        // Load first frame.
        loadReaderNextFrame(pReader);

        while (moreBuildProcessing) {
            fillMemory();
            joinMemoryBlockWithRunFile(writer, pReader);

            // Clean up
            for (int pid : buildInMemoryPartitions) {
                buildBufferManager.clearPartition(pid);
            }
            buildInMemoryPartitions.clear();
        }
        pReader.close();
    }

    private void joinMemoryBlockWithRunFile(IFrameWriter writer, RunFileReader pReader) throws HyracksDataException {
        // Join Disk partitions with Memory partitions
        for (RunFilePointer probeId : probeRunFilePointers.navigableKeySet()) {
            Pair<Integer, Integer> probe =
                    OverlappingIntervalPartitionUtil.getIntervalPartition(probeRunFilePointers.get(probeId), k);

            // Determine partitions to join.
            for (int buildId : buildInMemoryPartitions) {
                Pair<Integer, Integer> build = OverlappingIntervalPartitionUtil.getIntervalPartition(buildId, k);
                if (!(probe.first == 0 && build.first == 0)
                        && imjc.compareIntervalPartition(probe.first, probe.second, build.first, build.second)) {
                    fbms.add(buildBufferManager.getPartitionFrameBufferManager(buildId));
                    //                    System.err.println("join " + probe + " (" + probePartitionSizes[probeRunFilePointers.get(probeId)]
                    //                            + ") with (" + buildPartitionSizes[buildId] + "), ");
                }
            }

            if (!fbms.isEmpty()) {
                joinMemoryPartitionsWithRunFile(pReader, probeId, fbms, writer);
                //            } else {
                //                System.err.print("none");
            }

            fbms.clear();
        }
    }

    private void joinMemoryPartitionsWithRunFile(RunFileReader pReader, RunFilePointer rfpStart,
            List<IFrameBufferManager> buildFbms, IFrameWriter writer) throws HyracksDataException {
        long fileOffsetStart = rfpStart.getFileOffset();
        int tupleStart = rfpStart.getTupleIndex();

        RunFilePointer rfpEnd = probeRunFilePointers.higherKey(rfpStart);
        long fileOffsetEnd = rfpEnd == null ? pReader.getFileSize() : rfpEnd.getFileOffset();
        int tupleEnd = rfpEnd == null ? Integer.MAX_VALUE : rfpEnd.getTupleIndex();

        if (pReader.getReadPointer() != fileOffsetStart) {
            pReader.reset(fileOffsetStart);
            loadReaderNextFrame(pReader);
        }
        do {
            int start = pReader.getReadPointer() == fileOffsetStart ? tupleStart : 0;
            int end = pReader.getReadPointer() == fileOffsetEnd ? tupleEnd : accessorProbe.getTupleCount();

            for (int i = start; i < end; ++i) {
                // Tuple has potential match from build phase
                for (IFrameBufferManager fbm : buildFbms) {
                    joinTupleWithMemoryPartition(accessorProbe, i, fbm, writer);
                }
            }
        } while (pReader.getReadPointer() < fileOffsetEnd && loadReaderNextFrame(pReader));
    }

    private boolean loadReaderNextFrame(RunFileReader pReader) throws HyracksDataException {
        if (pReader.nextFrame(reloadBuffer)) {
            accessorProbe.reset(reloadBuffer.getBuffer());
            spillReadCount++;
            return true;
        }
        return false;
    }

    private void joinTupleWithMemoryPartition(IFrameTupleAccessor accessorProbe, int probeTupleIndex,
            IFrameBufferManager fbm, IFrameWriter writer) throws HyracksDataException {
        if (fbm.getNumFrames() == 0) {
            return;
        }
        fbm.resetIterator();
        int frameIndex = fbm.next();
        while (fbm.exists()) {
            fbm.getFrame(frameIndex, bufferInfo);
            accessorBuild.reset(bufferInfo.getBuffer());
            for (int buildTupleIndex = 0; buildTupleIndex < accessorBuild.getTupleCount(); ++buildTupleIndex) {
                if (imjc.checkToSaveInResult(accessorProbe, probeTupleIndex, accessorBuild, buildTupleIndex)) {
                    appendToResult(accessorBuild, buildTupleIndex, accessorProbe, probeTupleIndex, writer);
                }
                joinComparisonCount++;
            }
            frameIndex = fbm.next();
        }
    }

    private void appendToResult(IFrameTupleAccessor accessorBuild, int buildSidetIx, IFrameTupleAccessor accessorProbe,
            int probeSidetIx, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessorProbe, probeSidetIx, accessorBuild,
                buildSidetIx);
        joinResultCount++;
    }

    private void fillMemory() throws HyracksDataException {
        // TODO Add check this partition is needed for a join.
        int buildPid = -1;
        TupleStatus ts = loadRightTuple();
        while (ts.isLoaded() && inputAccessor[RIGHT_PARTITION].exists()) {
            int pid =
                    buildHpc.partition(inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId(), k);

            if (buildPid > 0 && buildPid != pid) {
                // Only add one partition
                return;
            }

            //            System.err.println("Tuple count:" + inputAccessor[RIGHT_PARTITION].getTupleCount() + " Tuple size:"
            //                    + inputAccessor[RIGHT_PARTITION].getTupleLength());
            if (!buildBufferManager.insertTuple(pid, inputAccessor[RIGHT_PARTITION],
                    inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr)) {
                return;
            }
            buildPartitionSizes[pid]++;

            if (buildPid != pid) {
                // Track new partitions in memory.
                buildInMemoryPartitions.add(pid);
                buildPid = pid;
            }
            inputAccessor[RIGHT_PARTITION].next();
            ts = loadRightTuple();
        }
        if (ts.isEmpty()) {
            moreBuildProcessing = false;
        }
    }

    private TupleStatus loadRightTuple() throws HyracksDataException {
        return loadMemoryTuple(RIGHT_PARTITION);
    }

}
