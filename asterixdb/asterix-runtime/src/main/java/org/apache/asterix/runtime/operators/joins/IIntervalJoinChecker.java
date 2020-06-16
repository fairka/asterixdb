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

import java.io.Serializable;

import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public interface IIntervalJoinChecker extends Serializable {

    /**
     * Check to see if the right tuple should be added to memory during the merge join.
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple could still match with this right tuple.
     *
     * @param accessorLeft
     * @param accessorRight
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToSaveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight) throws HyracksDataException;

    boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    /**
     * Check to see if the right tuple should be removed from memory during the merge join.
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple is NOT able match with this right tuple.
     *
     * @param accessorLeft
     * @param accessorRight
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToRemoveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException;

    boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    /**
     * Check to see if the next right tuple should be loaded during the merge join.
     * The check is true if the left tuple could match with the next right tuple.
     * Once the left tuple can no long match, the check returns false.
     *
     * @param accessorLeft
     * @param accessorRight
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToLoadNextRightTuple(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException;

    /**
     * Check to see if the left tuple should continue checking for matches.
     * The check is true if the next left tuple is NOT able match with this right tuple.
     *
     * @param accessorLeft
     * @param accessorRight
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkIfMoreMatches(ITupleAccessor accessorLeft, ITupleAccessor accessorRight) throws HyracksDataException;

    boolean checkIfMoreMatches(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    boolean checkToSaveInResult(ITupleAccessor accessorLeft, ITupleAccessor accessorRight) throws HyracksDataException;

    boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex, boolean reversed) throws HyracksDataException;

    public boolean checkToRemoveLeftActive();

    public boolean checkToRemoveRightActive();

    public boolean checkToIncrementMerge(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException;

    public boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight) throws HyracksDataException;

    public boolean compareIntervalPartition(int s1, int e1, int s2, int e2);

    public boolean checkToSaveInResult(long start0, long end0, long start1, long end1, boolean reversed);

    boolean compareInterval(long start0, long end0, long start1, long end1) throws HyracksDataException;

    boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    boolean checkForEarlyExit(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    boolean checkToRemoveInMemory(long start0, long start1);

}
