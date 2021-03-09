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

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.joins.interval.TimeSweep.memory.VPartitionDeletableTupleBufferManager;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ActiveSweepManagerTest {

    private final ISerializerDeserializer<AInterval> intervalSerde = AIntervalSerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers = new ISerializerDeserializer[] {
            AIntervalSerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    private final int FRAME_SIZE = 320;

    private final AInterval[] INTERVALS = new AInterval[] { new AInterval(99, 301, (byte) 16),
            new AInterval(100, 300, (byte) 16), new AInterval(101, 299, (byte) 16) };
    private final int INTERVAL_LENGTH = Byte.BYTES + Long.BYTES + Long.BYTES;

    /*
     * Tests
     * 
     * - add
     * - top point
     * - remove
     * - has records
     * - is empty
     * - clear
     */
    IPartitionedDeletableTupleBufferManager bufferManager;
    FrameTupleCursor memoryAccessor;
    FrameTupleAppender appender;

    @SuppressWarnings("unused")
    private byte[] getIntervalBytes(AInterval[] intervals) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < intervals.length; ++i) {
                dos.write(ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                intervalSerde.serialize(intervals[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, AInterval[] intervals) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender fffta = new FrameTupleAppender();
        fffta.reset(frame, true);

        byte[] serializedIntervals = getIntervalBytes(intervals);
        for (int i = 0; i < intervals.length; ++i) {
            fffta.append(serializedIntervals, i * INTERVAL_LENGTH, INTERVAL_LENGTH);
        }

        return frame.getBuffer();
    }

    private void prepareMemory(ActiveSweepManager activeManager) throws HyracksDataException {
        FrameTupleCursor accessor = new FrameTupleCursor(RecordDesc);
        ByteBuffer buffer = prepareData(ctx, INTERVALS);
        accessor.reset(buffer);
        TuplePointer tp = new TuplePointer();
        accessor.next();
        activeManager.addTuple(accessor, tp);

        tp = new TuplePointer();
        accessor.next();
        activeManager.addTuple(accessor, tp);

        tp = new TuplePointer();
        accessor.next();
        activeManager.addTuple(accessor, tp);
    }

    IHyracksTaskContext ctx;

    @Before
    public void initial() throws HyracksDataException {
        int memorySize = 5;
        ctx = TestUtils.create(FRAME_SIZE);
        RecordDescriptor intervalRD = new RecordDescriptor(
                new ISerializerDeserializer[] { AIntervalSerializerDeserializer.INSTANCE });
        RecordDescriptor[] records = { intervalRD, intervalRD };
        bufferManager = new VPartitionDeletableTupleBufferManager(ctx,
                VPartitionDeletableTupleBufferManager.NO_CONSTRAIN, 2, memorySize * ctx.getInitialFrameSize(), records);
        memoryAccessor = new FrameTupleCursor(intervalRD);
        appender = new FrameTupleAppender();
    }

    @Test
    public void testBasicOperations() throws HyracksDataException {
        ActiveSweepManager activeManager = new ActiveSweepManager(bufferManager, 0, 0,
                EndPointIndexItem.EndPointAscComparator, EndPointIndexItem.START_POINT);

        assertEquals(activeManager.hasRecords(), false);
        assertEquals(activeManager.isEmpty(), true);

        prepareMemory(activeManager);

        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        activeManager.clear();
        assertEquals(0, activeManager.getActiveList().size());
        assertEquals(false, activeManager.hasRecords());
        assertEquals(true, activeManager.isEmpty());

    }

    @Test
    public void testAscendingStartpoint() throws HyracksDataException {
        ActiveSweepManager activeManager = new ActiveSweepManager(bufferManager, 0, 0,
                EndPointIndexItem.EndPointAscComparator, EndPointIndexItem.START_POINT);

        assertEquals(activeManager.hasRecords(), false);
        assertEquals(activeManager.isEmpty(), true);

        prepareMemory(activeManager);

        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        assertEquals(99, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(100, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(101, activeManager.getTopPoint());
        activeManager.removeTop();

        prepareMemory(activeManager);
        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        activeManager.clear();
        assertEquals(0, activeManager.getActiveList().size());
        assertEquals(false, activeManager.hasRecords());
        assertEquals(true, activeManager.isEmpty());
    }

    @Test
    public void testDescendingStartpoint() throws HyracksDataException {
        ActiveSweepManager activeManager = new ActiveSweepManager(bufferManager, 0, 0,
                EndPointIndexItem.EndPointDescComparator, EndPointIndexItem.START_POINT);

        assertEquals(activeManager.hasRecords(), false);
        assertEquals(activeManager.isEmpty(), true);

        prepareMemory(activeManager);

        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        assertEquals(101, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(100, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(99, activeManager.getTopPoint());
        activeManager.removeTop();

        prepareMemory(activeManager);
        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        activeManager.clear();
        assertEquals(0, activeManager.getActiveList().size());
        assertEquals(false, activeManager.hasRecords());
        assertEquals(true, activeManager.isEmpty());
    }

    @Test
    public void testAscendingEndpoint() throws HyracksDataException {
        ActiveSweepManager activeManager = new ActiveSweepManager(bufferManager, 0, 0,
                EndPointIndexItem.EndPointAscComparator, EndPointIndexItem.END_POINT);

        assertEquals(activeManager.hasRecords(), false);
        assertEquals(activeManager.isEmpty(), true);

        prepareMemory(activeManager);

        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        assertEquals(299, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(300, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(301, activeManager.getTopPoint());
        activeManager.removeTop();

        prepareMemory(activeManager);
        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        activeManager.clear();
        assertEquals(0, activeManager.getActiveList().size());
        assertEquals(false, activeManager.hasRecords());
        assertEquals(true, activeManager.isEmpty());
    }

    @Test
    public void testDescendingEndpoint() throws HyracksDataException {
        ActiveSweepManager activeManager = new ActiveSweepManager(bufferManager, 0, 0,
                EndPointIndexItem.EndPointDescComparator, EndPointIndexItem.END_POINT);

        assertEquals(activeManager.hasRecords(), false);
        assertEquals(activeManager.isEmpty(), true);

        prepareMemory(activeManager);

        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        assertEquals(301, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(300, activeManager.getTopPoint());
        activeManager.removeTop();
        assertEquals(299, activeManager.getTopPoint());
        activeManager.removeTop();

        prepareMemory(activeManager);
        assertEquals(3, activeManager.getActiveList().size());
        assertEquals(true, activeManager.hasRecords());
        assertEquals(false, activeManager.isEmpty());

        activeManager.clear();
        assertEquals(0, activeManager.getActiveList().size());
        assertEquals(false, activeManager.hasRecords());
        assertEquals(true, activeManager.isEmpty());
    }

}
