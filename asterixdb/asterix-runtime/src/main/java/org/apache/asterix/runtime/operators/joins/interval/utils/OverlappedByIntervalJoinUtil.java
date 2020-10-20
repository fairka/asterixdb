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
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionLogic;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class OverlappedByIntervalJoinUtil extends AbstractIntervalInverseJoinUtil {

    public OverlappedByIntervalJoinUtil(int buildKey, int probeKey) {
        super(buildKey, probeKey);
    }

    @Override
    public boolean compareInterval(AIntervalPointable ipBuild, AIntervalPointable ipProbe) throws HyracksDataException {
        return il.overlappedBy(ipBuild, ipProbe);
    }

    @Override
    public boolean compareIntervalPartition(int s1, int e1, int s2, int e2) {
        return IntervalPartitionLogic.overlappedBy(s1, e1, s2, e2);
    }
}
