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
package org.apache.hyracks.dataflow.std.join;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class TuplePrinterUtil {

    private TuplePrinterUtil() {
    }

    public static long[] returnTupleFieldsAsBigInts(ITupleAccessor accessor) {
        if (accessor.exists()) {
            int tupleId = accessor.getTupleId();
            int fields = accessor.getFieldCount();
            long[] fieldLongs = new long[fields];
            for (int i = 0; i < fields; i++) {
                int fieldStartOffset = accessor.getFieldStartOffset(tupleId, i);
                int fieldSlotsLength = accessor.getFieldSlotsLength();
                int tupleStartOffset = accessor.getTupleStartOffset(tupleId);

                int start = fieldStartOffset + fieldSlotsLength + tupleStartOffset;
                int end = start + accessor.getFieldLength(tupleId, i);

                byte[] fieldValueBytes = Arrays.copyOfRange(accessor.getBuffer().array(), start, end);

                // https://stackoverflow.com/a/1026804
                fieldLongs[i] = 0;
                for (int j = 0; j < fieldValueBytes.length; j++) {
                    fieldLongs[i] = (fieldLongs[i] << 8) + (fieldValueBytes[j] & 0xff);
                }
            }
            return fieldLongs;
        }
        return new long[0];
    }

    public static String[] printTuple(String message, ITupleAccessor accessor) throws HyracksDataException {
        if (accessor.exists()) {
            return printTuple(message, accessor, accessor.getTupleId());
        } else {
            System.err.print(String.format("%1$-" + 15 + "s", message) + " --");
            System.err.print("no tuple");
            System.err.println();
            return new String[0];
        }
    }

    public static String[] printTuple(String message, IFrameTupleAccessor accessor, int tupleId)
            throws HyracksDataException {
        System.err.print(String.format("%1$-" + 15 + "s", message) + " --");
        int fields = accessor.getFieldCount();
        String[] fieldStrings = new String[fields];
        for (int i = 0; i < fields; ++i) {
            System.err.print(" " + i + ": ");
            int fieldStartOffset = accessor.getFieldStartOffset(tupleId, i);
            int fieldSlotsLength = accessor.getFieldSlotsLength();
            int tupleStartOffset = accessor.getTupleStartOffset(tupleId);

            int start = fieldStartOffset + fieldSlotsLength + tupleStartOffset;
            int end = start + accessor.getFieldLength(tupleId, i);
            //
            String fieldString = Arrays.toString(Arrays.copyOfRange(accessor.getBuffer().array(), start, end));
            //            long fieldInteger = getIntegerFromBytes(fieldBuffer);
            //            System.err.print("" + fieldBuffer);
            System.err.print(fieldString);
            fieldStrings[i] = fieldString;
        }
        System.err.println();
        return fieldStrings;
    }

    private static long getIntegerFromBytes(byte[] fieldBytes) throws HyracksDataException {
        ByteArrayInputStream bis = new ByteArrayInputStream(fieldBytes);
        DataInputStream dis = new DataInputStream(bis);
        Integer64SerializerDeserializer int64SerDe = Integer64SerializerDeserializer.INSTANCE;
        return int64SerDe.deserialize(dis);
    }

}
