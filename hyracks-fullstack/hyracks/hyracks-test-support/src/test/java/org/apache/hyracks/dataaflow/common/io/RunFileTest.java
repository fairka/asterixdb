package org.apache.hyracks.dataaflow.common.io;

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
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for RunFileReader and RunFileWriter.
 *
 * Runs test for single and multiple frames written to the run file.
 * The reading can be done in sequence, reverse and in separate sessions between writing.
 *
 * @see org.apache.hyracks.dataflow.common.io.RunFileReader
 * @see org.apache.hyracks.dataflow.common.io.RunFileWriter
 */
public class RunFileTest {

    private static final int INPUT_BUFFER_SIZE = 4096;
    private static final int TEST_FRAME_SIZE = 256;
    private static final int TEST_FRAME_SIZE_ALTERNATE = 512;
    private static final int MULTIPLE_FRAMES = 5;
    private static final int NUMBER_OF_TUPLES = 10;
    private static final int NUMBER_OF_TUPLES_ALTERNATE = 100;

    private final IHyracksTaskContext ctx = TestUtils.create(TEST_FRAME_SIZE);

    private RunFileReader reader;
    private RunFileWriter writer;

    @Before
    public void setup() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("RunFileTest");
        IIOManager ioManager = ctx.getIoManager();
        writer = new RunFileWriter(file, ioManager);
    }

    @SuppressWarnings("unused")
    private byte[] getIntegerBytes(Integer[] numbers) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < numbers.length; ++i) {
                dos.writeLong(numbers[i].longValue());
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private IFrame[] getFrames(int count) throws HyracksDataException {
        IFrame[] frames = new IFrame[count];
        for (int f = 0; f < frames.length; f++) {
            frames[f] = new VSizeFrame(ctx);
        }
        return frames;
    }

    private IFrame[] getFramesAlternate(int count) throws HyracksDataException {
        if (count < 2) {
            throw new HyracksDataException("Must provided more that one frames.");
        }
        IFrame[] frames = new IFrame[count];
        frames[0] = new VSizeFrame(ctx);
        frames[1] = new VSizeFrame(ctx, TEST_FRAME_SIZE_ALTERNATE);
        for (int f = 2; f < frames.length; f++) {
            frames[f] = new VSizeFrame(ctx);
        }
        return frames;
    }

    @Test
    public void testSingleFrame() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        accessor.reset(frame.getBuffer());
        FrameTupleReference tuple = new FrameTupleReference();

        appender.reset(frame, true);

        for (int i = 0; i < NUMBER_OF_TUPLES; i++) {

            int f0 = rnd.nextInt() % 100000;
            int f1 = 5;

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                break;
            }
        }

        IFrame readFrame = new VSizeFrame(ctx);

        // Writer test
        writer.open();
        writer.nextFrame(frame.getBuffer());

        // Reading what was written
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        reader.nextFrame(readFrame);

        Assert.assertArrayEquals("Reading frame bytes", frame.getBuffer().array(), readFrame.getBuffer().array());

        reader.close();
    }

    @Test
    public void testMultipleFrame() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame[] frames = getFrames(MULTIPLE_FRAMES);
        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        //FrameTupleReference tuple = new FrameTupleReference();

        // Writer test
        writer.open();

        for (int f = 0; f < frames.length; f++) {
            accessor.reset(frames[f].getBuffer());
            appender.reset(frames[f], true);

            for (int i = 0; i < NUMBER_OF_TUPLES; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            writer.nextFrame(frames[f].getBuffer());
        }
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }

    @Test
    public void testMultipleFrameMultipleReads() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame[] frames = getFrames(MULTIPLE_FRAMES);
        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        //FrameTupleReference tuple = new FrameTupleReference();

        // Writer test
        writer.open();

        for (int f = 0; f < frames.length; f++) {
            accessor.reset(frames[f].getBuffer());
            appender.reset(frames[f], true);

            for (int i = 0; i < NUMBER_OF_TUPLES; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            writer.nextFrame(frames[f].getBuffer());
        }
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.seek(0);
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.seek(0);
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }

    @Test
    public void testMultipleFrameRandomReads() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame[] frames = getFrames(MULTIPLE_FRAMES);
        long[] frameOffets = new long[frames.length];

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        //FrameTupleReference tuple = new FrameTupleReference();

        // Writer test
        writer.open();

        for (int f = 0; f < frames.length; f++) {
            accessor.reset(frames[f].getBuffer());
            appender.reset(frames[f], true);

            for (int i = 0; i < NUMBER_OF_TUPLES; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            frameOffets[f] = writer.getFileSize();
            writer.nextFrame(frames[f].getBuffer());
        }
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        for (int f = frames.length - 1; f >= 0; f--) {
            reader.seek(frameOffets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }

    @Test
    public void testMultipleDifferentSizedFrames() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame[] frames = getFramesAlternate(MULTIPLE_FRAMES);
        long[] frameOffets = new long[frames.length];

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        //FrameTupleReference tuple = new FrameTupleReference();

        // Writer test
        writer.open();

        for (int f = 0; f < frames.length; f++) {
            accessor.reset(frames[f].getBuffer());
            appender.reset(frames[f], false);

            int tupleCount = (f == 1 ? NUMBER_OF_TUPLES_ALTERNATE : NUMBER_OF_TUPLES);
            for (int i = 0; i < tupleCount; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            frameOffets[f] = writer.getFileSize();
            writer.nextFrame(frames[f].getBuffer());
        }
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        for (int f = frames.length - 1; f >= 0; f--) {
            reader.seek(frameOffets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }

    @Test
    public void testMultipleWriteReadSessions() throws HyracksDataException {
        Random rnd = new Random();
        rnd.setSeed(50);

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        IFrame[] framesAll = getFrames(MULTIPLE_FRAMES * 2);
        IFrame[] readFrames = getFrames(framesAll.length);
        long[] frameOffets = new long[framesAll.length];

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recDesc);
        //FrameTupleReference tuple = new FrameTupleReference();

        // Writer test
        writer.open();

        for (int f = 0; f < MULTIPLE_FRAMES; f++) {
            accessor.reset(framesAll[f].getBuffer());
            appender.reset(framesAll[f], true);

            for (int i = 0; i < NUMBER_OF_TUPLES; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            frameOffets[f] = writer.getFileSize();
            writer.nextFrame(framesAll[f].getBuffer());
        }

        // Reading what was written
        reader = writer.createReader();
        reader.open();
        for (int f = 0; f < MULTIPLE_FRAMES; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        for (int f = MULTIPLE_FRAMES - 1; f >= 0; f--) {
            reader.seek(frameOffets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();

        // Second write session
        for (int f = MULTIPLE_FRAMES; f < MULTIPLE_FRAMES * 2; f++) {
            accessor.reset(framesAll[f].getBuffer());
            appender.reset(framesAll[f], true);

            for (int i = 0; i < NUMBER_OF_TUPLES; i++) {
                int f0 = rnd.nextInt() % 100000;
                int f1 = 5;

                tb.reset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
                tb.addFieldEndOffset();
                IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
                tb.addFieldEndOffset();

                if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                }
            }
            frameOffets[f] = writer.getFileSize();
            writer.nextFrame(framesAll[f].getBuffer());
        }

        // Reading what was written in both sections
        reader = writer.createDeleteOnCloseReader();
        reader.open();
        for (int f = 0; f < MULTIPLE_FRAMES * 2; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        for (int f = MULTIPLE_FRAMES * 2 - 1; f >= 0; f--) {
            reader.seek(frameOffets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }

    //    public void readFrames(ITypeTraits[] readFrames, IFrame[] framesAll) throws HyracksDataException {
    //        reader = writer.createDeleteOnCloseReader();
    //        reader.open();
    //        for (int f = 0; f < MULTIPLE_FRAMES * 2; f++) {
    //            reader.nextFrame(readFrames[f]);
    //            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
    //                    readFrames[f].getBuffer().array());
    //        }
    //        for (int f = MULTIPLE_FRAMES * 2 - 1; f >= 0; f--) {
    //            reader.reset(readFrames[f]);
    //            reader.nextFrame(readFrames[f]);
    //            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
    //                    readFrames[f].getBuffer().array());
    //        }
    //        reader.close();
    //    }

}
