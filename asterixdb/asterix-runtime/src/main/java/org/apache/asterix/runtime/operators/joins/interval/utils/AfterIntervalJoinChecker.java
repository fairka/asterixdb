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

import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class AfterIntervalJoinChecker extends AbstractIntervalJoinChecker {
    private static final long serialVersionUID = 1L;

    public AfterIntervalJoinChecker(int[] keysLeft, int[] keysRight) {
        super(keysLeft[0], keysRight[0]);
    }

    @Override
    public boolean checkToSaveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
        IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
        ipLeft.getStart(startLeft);
        ipRight.getStart(startRight);
        return ch.compare(ipLeft.getByteArray(), ipLeft.getStartOffset(), ipLeft.getLength(), ipRight.getByteArray(),
                ipRight.getStartOffset(), ipRight.getLength()) >= 0;
    }

    //THis needs to be fixed
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        return false;
    }

    @Override
    public boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight) throws HyracksDataException {
        return il.after(ipLeft, ipRight);
    }

    @Override
    public boolean checkForEarlyExit(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        return false;
    }

    @Override
    public boolean checkIfMoreMatches(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        return true;
    }

    @Override
    public boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        return true;
    }

}
