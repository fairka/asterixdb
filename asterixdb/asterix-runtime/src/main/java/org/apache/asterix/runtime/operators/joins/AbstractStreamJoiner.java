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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.join.IStreamJoiner;
import org.apache.hyracks.dataflow.std.join.JoinData;

public abstract class AbstractStreamJoiner implements IStreamJoiner {

    public enum TupleStatus {
        UNKNOWN,
        LOADED,
        EMPTY;

        public boolean isLoaded() {
            return this.equals(LOADED);
        }

        public boolean isEmpty() {
            return this.equals(EMPTY);
        }

        public boolean isKnown() {
            return !this.equals(UNKNOWN);
        }
    }

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final ITupleAccessor[] inputAccessor;
    protected final JoinData[] joinData;

    public AbstractStreamJoiner(IHyracksTaskContext ctx, JoinData leftJoinData, JoinData rightJoinData)
            throws HyracksDataException {

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftJoinData.getRecordDescriptor());
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightJoinData.getRecordDescriptor());

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[RIGHT_PARTITION] = new VSizeFrame(ctx);

        joinData = new JoinData[JOIN_PARTITIONS];
        joinData[LEFT_PARTITION] = leftJoinData;
        joinData[RIGHT_PARTITION] = rightJoinData;

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

}