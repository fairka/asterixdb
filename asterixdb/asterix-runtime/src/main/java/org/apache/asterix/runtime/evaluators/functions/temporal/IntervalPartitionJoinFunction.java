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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.operators.joins.interval.overlappingintervalpartition.OverlappingIntervalPartitionUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor.RangeForwardTaskState;

@Deprecated
//Needs to be removed
public class IntervalPartitionJoinFunction implements IScalarEvaluator {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private DataOutput out = resultStorage.getDataOutput();
    private IPointable argPtr0 = new VoidPointable();
    private IPointable argPtr1 = new VoidPointable();
    private IPointable argPtr2 = new VoidPointable();
    private int rangeIdCache = -1;
    private long partitionStart;
    private long partitionDuration;
    private int partitionId;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt32> intSerde =
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    private AMutableInt32 aInt = new AMutableInt32(0);

    private IEvaluatorContext ctx;

    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private boolean startPoint;

    public IntervalPartitionJoinFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, boolean startPoint)
            throws HyracksDataException {
        this.ctx = ctx;
        this.eval0 = args[0].createScalarEvaluator(ctx);
        this.eval1 = args[1].createScalarEvaluator(ctx);
        this.eval2 = args[2].createScalarEvaluator(ctx);
        this.startPoint = startPoint;
        this.partitionId = ctx.getTaskContext().getTaskAttemptId().getTaskId().getPartition();
    }

    private void setPartitionLocalPartition(int rangeId, int k) throws HyracksDataException {
        System.err.println("IntervalPartitionJoinFunction get RangeId " + rangeId + " on partition " + partitionId);
        RangeForwardTaskState rangeState = RangeForwardTaskState.getRangeState(rangeId, ctx);
        RangeMap rangeMap = rangeState.getRangeMap();
        int nPartitions = rangeState.getNumberOfPartitions();

        partitionStart = OverlappingIntervalPartitionUtil.getPartitionStartValue(rangeMap, partitionId, nPartitions);
        long partitionEnd = OverlappingIntervalPartitionUtil.getPartitionEndValue(rangeMap, partitionId, nPartitions);
        partitionDuration = OverlappingIntervalPartitionUtil.getPartitionDuration(partitionStart, partitionEnd, k);
        System.err.println("IntervalPartitionJoinFunction partitions " + partitionId + ":" + nPartitions + " start "
                + partitionStart + " end " + partitionEnd + " duration " + partitionDuration);
    }

    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException,
            AlgebricksException {
        resultStorage.reset();
        // Interval
        eval0.evaluate(tuple, argPtr0);
        // rangeId
        eval1.evaluate(tuple, argPtr1);
        // k
        eval2.evaluate(tuple, argPtr2);

        byte[] bytes0 = argPtr0.getByteArray();
        int offset0 = argPtr0.getStartOffset();
        byte[] bytes1 = argPtr1.getByteArray();
        int offset1 = argPtr1.getStartOffset();
        byte[] bytes2 = argPtr2.getByteArray();
        int offset2 = argPtr2.getStartOffset();

        try {
            if (bytes0[offset0] != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                throw new AlgebricksException("Expected type INTERVAL for parameter 0 but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]));
            }

            int rangeId;
            if (bytes1[offset1] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                rangeId = AInt32SerializerDeserializer.getInt(bytes1, offset1 + 1);
            } else if (bytes1[offset1] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                rangeId = (int) AInt64SerializerDeserializer.getLong(bytes1, offset1 + 1);
            } else {
                throw new AlgebricksException("Expected type INT32 for parameter 1 but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]));
            }

            int k;
            if (bytes2[offset2] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                k = AInt32SerializerDeserializer.getInt(bytes2, offset2 + 1);
            } else if (bytes2[offset2] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                k = (int) AInt64SerializerDeserializer.getLong(bytes2, offset2 + 1);
            } else {
                throw new AlgebricksException("Expected type INT32 for parameter 2 but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[offset2]));
            }

            long point;
            if (startPoint) {
                point = AIntervalSerializerDeserializer.getIntervalStart(bytes0, offset0 + 1);
            } else {
                point = AIntervalSerializerDeserializer.getIntervalEnd(bytes0, offset0 + 1);
            }

            if (rangeId != rangeIdCache) {
                // Only load new values if the range changed.
                setPartitionLocalPartition(rangeId, k);
                rangeIdCache = rangeId;
            }

            int partition =
                    OverlappingIntervalPartitionUtil.getIntervalPartition(point, partitionStart, partitionDuration, k);
            aInt.setValue(partition);
            intSerde.serialize(aInt, out);
        } catch (HyracksDataException | AlgebricksException hex) {
            throw new AlgebricksException("Temp String");
        }
        result.set(resultStorage);
    }

}
