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

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalJoinCheckerFactory;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.join.IStreamJoiner;
import org.apache.hyracks.dataflow.std.join.JoinData;
import org.apache.hyracks.dataflow.std.join.RunFileStream;

public class IntervalForwardScanJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_INPUT_INDEX = 0;
    private static final int RIGHT_INPUT_INDEX = 1;
    protected static final int JOIN_INPUT_INDEX = 2;
    private final int memoryForJoinInFrames;
    private final IIntervalJoinCheckerFactory imjcf;

    private static final Logger LOGGER = Logger.getLogger(IntervalForwardScanJoinOperatorDescriptor.class.getName());
    private final int[] leftKeys;
    private final int[] rightKeys;

    public IntervalForwardScanJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryInFrames,
            int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor, IIntervalJoinCheckerFactory imjcf) {
        super(spec, 2, 1);
        outRecDescs[0] = recordDescriptor;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoinInFrames = memoryInFrames;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {

        ActivityId joinCacheLeftID = new ActivityId(getOperatorId(), LEFT_INPUT_INDEX);
        ActivityId joinCacheRightID = new ActivityId(getOperatorId(), RIGHT_INPUT_INDEX);
        ActivityId joinCacheID = new ActivityId(getOperatorId(), JOIN_INPUT_INDEX);

        IActivity joinCacheLeft = new JoinCacheActivityNode(joinCacheLeftID, joinCacheID);
        IActivity joinCacheRight = new JoinCacheActivityNode(joinCacheRightID, joinCacheID);
        IActivity joinerNode = new JoinerActivityNode(joinCacheID, joinCacheLeftID, joinCacheRightID);

        builder.addActivity(this, joinCacheLeft);
        builder.addSourceEdge(LEFT_INPUT_INDEX, joinCacheLeft, 0);

        builder.addActivity(this, joinCacheRight);
        builder.addSourceEdge(RIGHT_INPUT_INDEX, joinCacheRight, 0);

        builder.addActivity(this, joinerNode);
        builder.addTargetEdge(0, joinerNode, 0);

        builder.addBlockingEdge(joinCacheLeft, joinerNode);
        builder.addBlockingEdge(joinCacheRight, joinerNode);

    }

    public static class JoinCacheTaskState extends AbstractStateObject {
        private JoinData joinData;

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class JoinCacheActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public JoinCacheActivityNode(ActivityId id, ActivityId nljAid) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final IHyracksJobletContext jobletCtx = ctx.getJobletContext();
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

            return new AbstractUnaryInputSinkOperatorNodePushable() {

                private RunFileStream runFileStream;
                String key = "job" + getActivityId();
                private IntervalForwardScanBranchStatus status = new IntervalForwardScanBranchStatus();
                FrameTupleAccessor accessor = new FrameTupleAccessor(rd0);

                JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new JoinCacheTaskState(jobletCtx.getJobId(), new TaskId(getActivityId(), partition));
                    runFileStream = new RunFileStream(ctx, key, status);
                    runFileStream.createRunFileWriting();
                    runFileStream.startRunFileWriting();

                    //state.joinData = new ProducerConsumerFrame(rd0);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    for (int x = 0; x < accessor.getTupleCount(); x++) {
                        runFileStream.addToRunFile(accessor, x);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joinData = new JoinData(rd0, runFileStream);
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    runFileStream.removeRunFile();
                    runFileStream.close();
                }
            };
        }
    }

    private class JoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private final ActivityId leftActivityID;
        private final ActivityId rightActivityID;

        public JoinerActivityNode(ActivityId id, ActivityId leftID, ActivityId rightID) {
            super(id);
            leftActivityID = leftID;
            rightActivityID = rightID;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {

            return new JoinerOperator(ctx, partition, nPartitions, inputArity);
        }

        private class JoinerOperator extends AbstractUnaryOutputSourceOperatorNodePushable {
            //Get from State and pass it in here
            private final IHyracksTaskContext ctx;
            private final int partition;
            private final int nPartitions;
            private final int inputArity;
            private JoinCacheTaskState[] inputStates;

            public JoinerOperator(IHyracksTaskContext ctx, int partition, int nPartitions, int inputArity)
                    throws HyracksDataException {

                this.ctx = ctx;
                this.partition = partition;
                this.inputArity = inputArity;
                this.inputStates = new JoinCacheTaskState[JOIN_INPUT_INDEX];
                this.inputStates[LEFT_INPUT_INDEX] =
                        (JoinCacheTaskState) ctx.getStateObject(new TaskId(leftActivityID, partition));
                this.inputStates[RIGHT_INPUT_INDEX] =
                        (JoinCacheTaskState) ctx.getStateObject(new TaskId(rightActivityID, partition));
                this.nPartitions = nPartitions;
            }

            @Override
            public int getInputArity() {
                return inputArity;
            }

            @Override
            public void initialize() throws HyracksDataException {

                sleepUntilStateIsReady(LEFT_INPUT_INDEX);
                sleepUntilStateIsReady(RIGHT_INPUT_INDEX);

                try {
                    writer.open();
                    //Pass in Data
                    IStreamJoiner joiner = new IntervalForwardScanJoiner(ctx, inputStates[LEFT_INPUT_INDEX].joinData,
                            inputStates[RIGHT_INPUT_INDEX].joinData, memoryForJoinInFrames, partition, imjcf, leftKeys,
                            rightKeys, writer, nPartitions);
                    joiner.processJoin();
                } catch (Exception ex) {
                    writer.fail();
                    throw ex;
                } finally {
                    writer.close();
                }
            }

            private void sleepUntilStateIsReady(int stateIndex) {
                int sleep = 0;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                } while (inputStates[stateIndex] == null);
            }
        }
    }
}
