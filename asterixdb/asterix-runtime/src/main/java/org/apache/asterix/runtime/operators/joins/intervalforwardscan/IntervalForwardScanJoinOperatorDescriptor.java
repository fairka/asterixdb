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

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.intervalindex.IStreamJoiner;
import org.apache.asterix.runtime.operators.joins.intervalindex.ProducerConsumerFrameState;
import org.apache.hyracks.api.comm.IFrameWriter;
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
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class IntervalForwardScanJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_INPUT_INDEX = 0;
    private static final int RIGHT_INPUT_INDEX = 1;
    private static final int JOIN_ACTIVITY_INDEX = 2; //WHY DOES THIS EXIST IN THIS ONE BUT NOT THE OTHER?
    private final int memoryForJoinInFrames;
    private final IIntervalMergeJoinCheckerFactory comparatorFactories;

    private static final Logger LOGGER = Logger.getLogger(IntervalForwardScanJoinOperatorDescriptor.class.getName());
    private final int[] leftKeys;
    private final int[] rightKeys;

    public IntervalForwardScanJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoinInFrames,
            int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IIntervalMergeJoinCheckerFactory comparatorFactories) {
        super(spec, 2, 1);
        outRecDescs[0] = recordDescriptor;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoinInFrames = memoryForJoinInFrames;
        this.comparatorFactories = comparatorFactories;
    }

    @Override public void contributeActivities(IActivityGraphBuilder builder) {

        ActivityId leftAid = new ActivityId(odId, LEFT_INPUT_INDEX);
        ActivityId rightAid = new ActivityId(odId, RIGHT_INPUT_INDEX);
        ActivityId joinAid = new ActivityId(odId, JOIN_ACTIVITY_INDEX);
        ActivityId[] dataAids = { leftAid, rightAid };

        IActivity joinerNode = new JoinerActivityNode(joinAid, dataAids);

        builder.addActivity(this, joinerNode);

        builder.addSourceEdge(LEFT_INPUT_INDEX, joinerNode, LEFT_INPUT_INDEX);
        builder.addSourceEdge(RIGHT_INPUT_INDEX, joinerNode, RIGHT_INPUT_INDEX);
        builder.addTargetEdge(0, joinerNode, 0);

    }

    private class JoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId[] dataIds;

        public JoinerActivityNode(ActivityId id, ActivityId[] dataIds) {
            super(id);
            this.dataIds = dataIds;
        }

        @Override public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {

            RecordDescriptor[] inRecordDescs = new RecordDescriptor[inputArity];
            inRecordDescs[LEFT_INPUT_INDEX] = recordDescProvider.getInputRecordDescriptor(id, LEFT_INPUT_INDEX);
            inRecordDescs[RIGHT_INPUT_INDEX] = recordDescProvider.getInputRecordDescriptor(id, RIGHT_INPUT_INDEX);
            return new JoinerOperator(ctx, partition, inputArity, dataIds, inRecordDescs);
        }

        private class JoinerOperator extends AbstractUnaryOutputSourceOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final int inputArity;
            private final RecordDescriptor[] recordDescriptors;
            private final ActivityId[] dataIds;
            private ProducerConsumerFrameState[] inputStates;

            //            private final RecordDescriptor[] recordDescriptors;
            //            private JoinComparator[] tupleComparators;

            public JoinerOperator(IHyracksTaskContext ctx, int partition, int inputArity, ActivityId[] dataIds,
                    RecordDescriptor[] inRecordDesc) {
                this.ctx = ctx;
                this.partition = partition;
                this.inputArity = inputArity;
                this.recordDescriptors = inRecordDesc;
                this.dataIds = dataIds;
                this.inputStates = new ProducerConsumerFrameState[inputArity];
            }

            @Override public void initialize() throws HyracksDataException {

                sleepUntilStateIsReady(LEFT_INPUT_INDEX);
                sleepUntilStateIsReady(RIGHT_INPUT_INDEX);

                try {
                    //                    for (int i = 0; i < comparatorFactories.length; i++) {
                    //                        IBinaryComparator comparator = comparatorFactories[i].createBinaryComparator();
                    //                        tupleComparators[i] = new JoinComparator(comparator, leftKeys[i], rightKeys[i]);
                    //                    }
                    writer.open();
                    IStreamJoiner joiner = new IntervalForwardScanJoiner(ctx, inputStates[LEFT_INPUT_INDEX],
                            inputStates[RIGHT_INPUT_INDEX], memoryForJoinInFrames, partition, comparatorFactories, leftKeys, rightKeys);
                    joiner.processJoin(writer);
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

            @Override public IFrameWriter getInputFrameWriter(int index) {
                return new IFrameWriter() {
                    @Override public void open() throws HyracksDataException {
                        inputStates[index] = new ProducerConsumerFrameState(ctx.getJobletContext().getJobId(),
                                new TaskId(getActivityId(), partition), recordDescriptors[index]);
                    }

                    @Override public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        inputStates[index].putFrame(buffer);
                    }

                    @Override public void close() throws HyracksDataException {
                        inputStates[index].noMoreFrames();
                    }

                    @Override public void fail() throws HyracksDataException {
                        inputStates[index].noMoreFrames();
                    }
                };
            }
        }
    }
}