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

package org.apache.asterix.runtime.operators.join.interval;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.runtime.operators.join.IIntervalJoinChecker;
import org.apache.asterix.runtime.operators.join.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

class IntervalSideTuple {
    // Tuple access
    int fieldId;
    ITuplePointerAccessor accessor;
    int tupleIndex;
    int frameIndex = -1;

    // Join details
    final IIntervalJoinChecker imjc;

    // Interval details
    long start;
    long end;

    public IntervalSideTuple(IIntervalJoinChecker imjc, ITuplePointerAccessor accessor, int fieldId) {
        this.imjc = imjc;
        this.accessor = accessor;
        this.fieldId = fieldId;
    }

    public void setTuple(TuplePointer tp) {
        if (frameIndex != tp.getFrameIndex()) {
            accessor.reset(tp);
            frameIndex = tp.getFrameIndex();
        }
        tupleIndex = tp.getTupleIndex();
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public void loadTuple() {
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public int getTupleIndex() {
        return tupleIndex;
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

    public boolean hasMoreMatches(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkIfMoreMatches(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean compareJoin(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(accessor, tupleIndex, ist.accessor, ist.tupleIndex, false);
    }

    public boolean removeFromMemory(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToRemoveInMemory(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean checkForEarlyExit(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkForEarlyExit(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

}
