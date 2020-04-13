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

import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogicWithLong;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionLogic;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CoveredByIntervalMergeJoinChecker extends AbstractIntervalInverseMergeJoinChecker {
    private static final long serialVersionUID = 1L;

    public CoveredByIntervalMergeJoinChecker(int[] keysLeft, int[] keysRight) {
        super(keysLeft[0], keysRight[0]);
    }

    @Override
    public boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight) throws HyracksDataException {
        return il.coveredBy(ipLeft, ipRight);
    }

    @Override
    public boolean compareIntervalPartition(int s1, int e1, int s2, int e2) {
        return IntervalPartitionLogic.coveredBy(s1, e1, s2, e2);
    }

    @Override
    public boolean compareInterval(long start0, long end0, long start1, long end1) {
        return IntervalLogicWithLong.coveredBy(start0, end0, start1, end1);

    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return start0 <= end1;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return start0 > end1;

    }

}