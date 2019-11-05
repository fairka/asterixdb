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

package org.apache.asterix.om.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalEndpointAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalStartpointDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.*;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;

import junit.framework.TestCase;

public abstract class AbstractFieldRangeMultiPartitionComputerFactoryTest extends TestCase {

    private final AIntervalSerializerDeserializer intervalSerde = AIntervalSerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers =
            new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    IBinaryComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalAscPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalDescPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_ASC_MAX_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalEndpointAscPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_DESC_MAX_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalStartpointDescPartialBinaryComparatorFactory.INSTANCE };

    /*
     * These tests check the range partitioning types with various interval sizes and range map split points.
     * For each range type they check the ASCending and DESCending comparators for intervals with durations of D = 3, and
     * a range map of the overall range that has been split into N = 4 parts.
     * the test for the Split type also checks larger intervals and more splits on the range map to make sure it splits
     * correctly across many partitions, and within single partitions.
     *
     * The map of the partitions, listed as the rangeMap split points in ascending and descending orders:
     *
     * The following points (X) will be tested for these 4 partitions.
     *
     *     X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
     *        -----------------------|-----------------------|-------------------------|--------------------------
     *
     * The following points (X) will be tested for these 16 partitions.
     *
     *     X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
     *        -----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|------|------|------|------|-----
     *
     * N4                0          )[           1          )[           2            )[             3
     * N16     0  )[  1 )[  2 )[  3 )[  4 )[  5 )[  6 )[  7 )[  8 )[  9 )[  10 )[  11 )[  12 )[  13 )[  14 )[  15
     * ASC   0     25    50    75    100   125   150   175   200   225   250    275    300    325    350    375    400
     * DESC  400   375   350   325   300   275   250   225   200   175   150    125    100    75     50     25     0
     *
     * First and last partitions include all values less than and greater than min and max split points respectively.
     *
     * Both rangeMap partitions and test intervals are end exclusive.
     * an ascending test interval ending on 200 like (190, 200) is not in partition 8.
     * similarly, a descending test ending on 200 like (210, 200) is not in partition 8.
     */

    private final int FRAME_SIZE = 640;
    private final int INTEGER_LENGTH = Long.BYTES;
    private final int INTERVAL_LENGTH = 1 + 2 * INTEGER_LENGTH;

    // Tests points inside each partition.
    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15   };
    //points       {     20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l  };
    protected final Long[] EACH_PARTITION =
            new Long[] { 20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l };

    // Tests points at or near partition boundaries and at the ends of the partition range.
    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14        };
    //points       {    -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l       };
    protected final Long[] PARTITION_EDGE_CASES =
            new Long[] { -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l };

    // The map of the partitions, listed as the split points.
    // partitions   {  0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15,   16 };
    // map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
    protected final Long[] MAP_POINTS =
            new Long[] { 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l };

    /**
     * @param integers
     * @param duration
     * @return
     * @throws HyracksDataException
     */
    private byte[] getIntervalBytes(Long[] integers, long duration) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            AInterval[] intervals = getAIntervals(integers, duration);
            for (int i = 0; i < integers.length; ++i) {
                intervalSerde.serialize(intervals[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected RangeMap getRangeMap(Long[] integers) throws HyracksDataException {
        int offsets[] = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTERVAL_LENGTH;
        }
        return new RangeMap(1, getIntervalBytes(integers, 0), offsets);
    }

    private AInterval[] getAIntervals(Long[] integers, long duration) {
        AInterval[] intervals = new AInterval[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            intervals[i] = new AInterval(integers[i], integers[i] + duration, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG);
        }
        return intervals;
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, AInterval[] intervals) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());
        DataOutput dos = tb.getDataOutput();
        appender.reset(frame, true);

        for (int i = 0; i < intervals.length; ++i) {
            tb.reset();
            intervalSerde.serialize(intervals[i], dos);
            tb.addFieldEndOffset();
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        return frame.getBuffer();
    }

    protected void executeFieldRangeReplicatePartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, IBinaryComparatorFactory[] maxComparatorFactories,
            int nParts, int[][] results, long duration) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);
        int[] rangeFields = new int[] { 0 };

        ITupleMultiPartitionComputerFactory itmpcf = new FieldRangeFollowingPartitionComputerFactory(rangeFields,
                minComparatorFactories, rangeMapSupplier, sourceLocation);

        executeFieldRangeMultiPartitionTests(integers, itmpcf, nParts, results, duration);

    }

    protected void executeFieldRangeSplitPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, IBinaryComparatorFactory[] maxComparatorFactories,
            int nParts, int[][] results, long duration) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);
        int[] startFields = new int[] { 0 };
        int[] endFields = new int[] { 1 };

        ITupleMultiPartitionComputerFactory itmpcf = new FieldRangeIntersectPartitionComputerFactory(startFields,
                endFields, minComparatorFactories, rangeMapSupplier, sourceLocation);

        executeFieldRangeMultiPartitionTests(integers, itmpcf, nParts, results, duration);
    }

    protected void executeFieldRangeProjectPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, IBinaryComparatorFactory[] maxComparatorFactories,
            int nParts, int[][] results, long duration) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);
        int[] rangeFields = new int[] { 0 };

        ITuplePartitionComputerFactory itpcf = new FieldRangePartitionComputerFactory(rangeFields,
                minComparatorFactories, rangeMapSupplier, sourceLocation);

        executeFieldRangePartitionTests(integers, itpcf, nParts, results, duration);

    }

    protected void executeFieldRangeMultiPartitionTests(Long[] integers, ITupleMultiPartitionComputerFactory itmpcf,
            int nParts, int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);

        ITupleMultiPartitionComputer partitioner = itmpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        AInterval[] intervals = getAIntervals(integers, duration);
        ByteBuffer buffer = prepareData(ctx, intervals);
        accessor.reset(buffer);

        BitSet map = new BitSet(16);

        for (int i = 0; i < results.length; ++i) {
            map.clear();
            map = partitioner.partition(accessor, i, nParts);
            checkPartitionResult(intervals[i], results[i], map);
        }
    }

    protected void executeFieldRangePartitionTests(Long[] integers, ITuplePartitionComputerFactory itpcf, int nParts,
            int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);

        ITuplePartitionComputer partitioner = itpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        AInterval[] intervals = getAIntervals(integers, duration);
        ByteBuffer buffer = prepareData(ctx, intervals);
        accessor.reset(buffer);

        int partition;

        for (int i = 0; i < results.length; ++i) {
            partition = partitioner.partition(accessor, i, nParts);
            Assert.assertEquals("The partition for value (" + intervals[i].getIntervalStart() + ":"
                    + intervals[i].getIntervalEnd() + ") gives different number of partitions", results[i][0],
                    partition);
        }
    }

    private String getString(int[] results) {
        String result = "[";
        for (int i = 0; i < results.length; ++i) {
            result += results[i];
            if (i < results.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    private String getString(BitSet results) {
        int count = 0;
        String result = "[";
        for (int i = 0; i < results.size(); ++i) {
            if (results.get(i)) {
                if (count > 0) {
                    result += ", ";
                }
                result += i;
                count++;
            }
        }
        result += "]";
        return result;
    }

    private void checkPartitionResult(AInterval interval, int[] results, BitSet map) {
        Assert.assertFalse(
                "The map partition " + getString(map) + " and the results " + getString(results) + " do not match. 1",
                results.length == map.cardinality());
        for (int i = 0; i < results.length; ++i) {
            Assert.assertFalse("The map partition " + getString(map) + " and the results " + getString(results)
                    + " do not match. 2.", map.get(i));
        }
    }
}
