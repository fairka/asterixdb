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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

//import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor.RangeForwardTaskState;

public class OverlappingIntervalJoinCheckerFactory extends AbstractIntervalJoinCheckerFactory {
    private static final long serialVersionUID = 1L;
    private final RangeMap rangeMap;

    public OverlappingIntervalJoinCheckerFactory(RangeMap rangeMap) {
        this.rangeMap = rangeMap;
    }

    @Override
    public IIntervalJoinChecker createIntervalMergeJoinChecker(int[] keys0, int[] keys1, IHyracksTaskContext ctx,
            int nPartitions) throws HyracksDataException {
        int fieldIndex = 0;

        //        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
        //            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        //        }
        int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        int slot = rangeMap.getMinSlotFromPartition(partition, nPartitions);

        long partitionStart = Long.MIN_VALUE;
        // All lookups are on typed values.
        //Switch. Don't assume its DATETIME, TIME, or DATE
        //Get Chronon
        if (slot <= rangeMap.getSplitCount()) {
            if (rangeMap.getTag(0, 0) == ATypeTag.DATETIME.serialize()) {
                partitionStart =
                        LongPointable.getLong(rangeMap.getByteArray(), rangeMap.getStartOffset(fieldIndex, slot) + 1);
            } else {
                partitionStart = IntegerPointable.getInteger(rangeMap.getByteArray(),
                        rangeMap.getStartOffset(fieldIndex, slot) + 1);
            }
        }
        return new OverlappingIntervalJoinChecker(keys0, keys1, partitionStart);
    }

    @Override
    public IIntervalJoinChecker createIntervalInverseMergeJoinChecker(int[] keys0, int[] keys1, IHyracksTaskContext ctx,
            int nPartitions) throws HyracksDataException {
        return createIntervalMergeJoinChecker(keys0, keys1, ctx, nPartitions);
    }
}
