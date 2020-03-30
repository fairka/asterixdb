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
import java.util.Comparator;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.intervalindex.EndPointIndexItem;
import org.apache.asterix.runtime.operators.joins.intervalindex.IConsumerFrame;
import org.apache.asterix.runtime.operators.joins.intervalindex.IStreamJoiner;
import org.apache.asterix.runtime.operators.joins.intervalindex.ProducerConsumerFrameState;
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
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class IntervalForwardScanJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;
    private static final int JOIN_ACTIVITY_ID = 2;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int memoryForJoin;
    private final IIntervalMergeJoinCheckerFactory imjcf;

    private static final Logger LOGGER = Logger.getLogger(IntervalForwardScanJoinOperatorDescriptor.class.getName());

    public IntervalForwardScanJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin,
            int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IIntervalMergeJoinCheckerFactory imjcf) {
        super(spec, 2, 1);
        //recordDescriptors[0] = recordDescriptor;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoin = memoryForJoin;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId leftAid = new ActivityId(odId, LEFT_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, RIGHT_ACTIVITY_ID);
        ActivityId joinAid = new ActivityId(odId, JOIN_ACTIVITY_ID);
        ActivityId[] dataAids = { leftAid, rightAid };

        IActivity leftAN = new InputDataActivityNode(leftAid);
        IActivity rightAN = new InputDataActivityNode(rightAid);
        IActivity joinAN = new JoinerActivityNode(joinAid, dataAids);

        builder.addActivity(this, rightAN);
        builder.addSourceEdge(1, rightAN, 0);

        builder.addActivity(this, leftAN);
        builder.addSourceEdge(0, leftAN, 0);

        builder.addActivity(this, joinAN);
        builder.addTargetEdge(0, joinAN, 0);
    }

    private class JoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId[] dataIds;

        public JoinerActivityNode(ActivityId id, ActivityId[] dataIds) {
            super(id);
            this.dataIds = dataIds;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new JoinerOperator(ctx, partition, dataIds);
        }

        private class JoinerOperator extends AbstractUnaryOutputSourceOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final ActivityId[] dataIds;

            public JoinerOperator(IHyracksTaskContext ctx, int partition, ActivityId[] dataIds)
                    throws HyracksDataException {
                this.ctx = ctx;
                this.dataIds = dataIds;
                this.partition = partition;
            }

            @Override
            public void initialize() throws HyracksDataException {
                int sleep = 0;
                ProducerConsumerFrameState leftState;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    leftState = (ProducerConsumerFrameState) ctx.getStateObject(new TaskId(dataIds[0], partition));
                } while (leftState == null);
                sleep = 0;
                ProducerConsumerFrameState rightState;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    rightState = (ProducerConsumerFrameState) ctx.getStateObject(new TaskId(dataIds[1], partition));
                } while (rightState == null);

                byte point = imjcf.isOrderAsc() ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;
                Comparator<EndPointIndexItem> endPointComparator = imjcf.isOrderAsc()
                        ? EndPointIndexItem.EndPointAscComparator : EndPointIndexItem.EndPointDescComparator;

                try {
                    writer.open();
                    IStreamJoiner joiner = new IntervalForwardScanJoiner(ctx, memoryForJoin, partition, imjcf, leftKeys,
                            rightKeys, (IConsumerFrame) leftState, (IConsumerFrame) rightState);
                    joiner.processJoin(writer);
                    leftState.close();
                    rightState.close();
                } catch (Exception ex) {
                    writer.fail();
                    throw new HyracksDataException("ex");
                } finally {
                    writer.close();
                }
            }
        }
    }

    private class InputDataActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public InputDataActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(id, 0);
            return new InputDataOperator(ctx, partition, inRecordDesc);
        }

        private class InputDataOperator extends AbstractUnaryInputSinkOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final RecordDescriptor recordDescriptor;
            private ProducerConsumerFrameState state;

            public InputDataOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc) {
                this.ctx = ctx;
                this.partition = partition;
                this.recordDescriptor = inRecordDesc;
            }

            @Override
            public void open() throws HyracksDataException {
                state = new ProducerConsumerFrameState(ctx.getJobletContext().getJobId(),
                        new TaskId(getActivityId(), partition), recordDescriptor);
                ctx.setStateObject(state);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                state.putFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                state.noMoreFrames();
            }

            @Override
            public void close() throws HyracksDataException {
                state.noMoreFrames();
            }
        }
    }
}