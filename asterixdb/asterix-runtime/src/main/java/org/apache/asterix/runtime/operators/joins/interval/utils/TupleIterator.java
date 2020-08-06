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
package org.apache.asterix.runtime.operators.joins.interval.utils;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class TupleIterator {
    public static final int UNSET = -2;
    public static final int INITIALIZED = -1;
    private int tupleId = UNSET;
    private IFrameTupleAccessor accessor;

    public TupleIterator(RecordDescriptor recordDescriptor) throws HyracksDataException {
        accessor = new FrameTupleAccessor(recordDescriptor);
    }

    public int getTupleId() {
        return tupleId;
    }

    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    public void next() {
        ++tupleId;
    }

    public boolean exists() {
        return INITIALIZED < tupleId && tupleId < accessor.getTupleCount();
    }

    public IFrameTupleAccessor getAccessor() {
        return accessor;
    }

    public void reset(ByteBuffer byteBuffer) {
        accessor.reset(byteBuffer);
        tupleId = INITIALIZED;
    }

    public void reset() {
        tupleId = INITIALIZED;
    }
}
