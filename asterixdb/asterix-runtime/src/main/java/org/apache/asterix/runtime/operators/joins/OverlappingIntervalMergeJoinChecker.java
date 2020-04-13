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
package org.apache.asterix.runtime.operators.joins;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogicWithLong;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionLogic;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class OverlappingIntervalMergeJoinChecker extends AbstractIntervalMergeJoinChecker {
    private static final long serialVersionUID = 1L;
    private final long partitionStart;

    public OverlappingIntervalMergeJoinChecker(int[] keysLeft, int[] keysRight, long partitionStart) {
        super(keysLeft[0], keysRight[0]);
        this.partitionStart = partitionStart;
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return start0 < end1;
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkForEarlyExit(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return end0 <= start1;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return start0 >= end1;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(long start0, long end1) {
        return start0 >= end1;
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex, boolean reversed) throws HyracksDataException {
        int offset0 = IntervalJoinUtil.getIntervalOffset(accessorLeft, leftTupleIndex, (reversed ? idRight : idLeft));
        int offset1 = IntervalJoinUtil.getIntervalOffset(accessorRight, rightTupleIndex, (reversed ? idLeft : idRight));

        long start0 = AIntervalSerializerDeserializer.getIntervalStart(accessorLeft.getBuffer().array(), offset0);
        long start1 = AIntervalSerializerDeserializer.getIntervalStart(accessorRight.getBuffer().array(), offset1);
        if (start0 < partitionStart && start1 < partitionStart) {
            // Both tuples will match in a different partition.
            return false;
        }
        long end0 = AIntervalSerializerDeserializer.getIntervalEnd(accessorLeft.getBuffer().array(), offset0);
        long end1 = AIntervalSerializerDeserializer.getIntervalEnd(accessorRight.getBuffer().array(), offset1);
        return IntervalLogicWithLong.overlapping(start0, end0, start1, end1);
    }

    public boolean checkToSaveInResult(long start0, long end0, long start1, long end1, boolean reversed) {
        if (start0 < partitionStart && start1 < partitionStart) {
            // Both tuples will match in a different partition.
            return false;
        }
        return super.checkToSaveInResult(start0, end0, start1, end1, reversed);
    }

    @Override
    public boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight) throws HyracksDataException {
        return il.overlapping(ipLeft, ipRight);
    }

    @Override
    public boolean compareIntervalPartition(int s1, int e1, int s2, int e2) {
        return IntervalPartitionLogic.overlapping(s1, e1, s2, e2);
    }

    @Override
    public boolean compareInterval(long start0, long end0, long start1, long end1) {
        return IntervalLogicWithLong.overlapping(start0, end0, start1, end1);
    }

}