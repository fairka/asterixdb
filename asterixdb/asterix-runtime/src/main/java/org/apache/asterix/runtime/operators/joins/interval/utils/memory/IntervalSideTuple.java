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

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IntervalSideTuple {
    // Tuple access
    int fieldId;
    IFrameTupleAccessor accessor;
    int tupleId = -1;

    long start;
    long end;

    // Join details
    final IIntervalJoinUtil imjc;

    public IntervalSideTuple(IIntervalJoinUtil imjc, IFrameTupleAccessor accessor, int fieldId) {
        this.imjc = imjc;
        this.accessor = accessor;
        this.fieldId = fieldId;
    }

    public void setTuple(int tupleId) {
        this.tupleId = tupleId;
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, this.tupleId, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public int getTupleIndex() {
        return tupleId;
    }

    public IFrameTupleAccessor getAccessor() {
        return accessor;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean compareJoin(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(accessor, tupleId, ist.accessor, ist.tupleId);
    }

    public boolean removeFromMemory(IntervalSideTuple ist) {
        return imjc.checkToRemoveInMemory(accessor, tupleId, ist.accessor, ist.tupleId);
    }

    public boolean checkForEarlyExit(IntervalSideTuple ist) {
        return imjc.checkForEarlyExit(accessor, tupleId, ist.accessor, ist.tupleId);
    }
}
