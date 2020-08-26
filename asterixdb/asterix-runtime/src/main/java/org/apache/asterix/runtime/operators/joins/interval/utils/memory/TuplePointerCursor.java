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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class TuplePointerCursor implements ITupleCursor {

    public static final int UNSET = -2;
    public static final int INITIALIZED = -1;
    public int tupleId = UNSET;
    ITuplePointerAccessor accessor;

    public TuplePointerCursor(ITuplePointerAccessor accessor) {
        this.accessor = accessor;
    }

    @Override
    public boolean exists() {
        return INITIALIZED < tupleId && tupleId < accessor.getTupleCount();
    }

    @Override
    public void next() {
        ++tupleId;
    }

    @Override
    public int getTupleId() {
        return tupleId;
    }

    @Override
    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    @Override
    public void reset(ByteBuffer byteBuffer) {
        throw new RuntimeException("This reset should never be called in ITuplePointerCursor.");
    }

    @Override
    public void reset(TuplePointer tp) {
        accessor.reset(tp);
    }

    @Override
    public IFrameTupleAccessor getAccessor() {
        return accessor;
    }
}
