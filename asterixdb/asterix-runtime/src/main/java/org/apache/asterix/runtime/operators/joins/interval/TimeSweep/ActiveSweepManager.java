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

package org.apache.asterix.runtime.operators.joins.interval.TimeSweep;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalJoinUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class ActiveSweepManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveSweepManager.class.getName());

    private final int partition;
    private final int key;
    private final byte indexPoint;

    private final IPartitionedDeletableTupleBufferManager bufferManager;
    private final PriorityQueue<EndPointIndexItem> indexQueue;
    private EndPointIndexItem item = null;
    private final LinkedList<TuplePointer> active = new LinkedList<>();

    public ActiveSweepManager(IPartitionedDeletableTupleBufferManager bufferManager, int key, int joinBranch,
            Comparator<EndPointIndexItem> endPointComparator, byte point) {
        this.bufferManager = bufferManager;
        this.key = key;
        this.partition = joinBranch;
        indexQueue = new PriorityQueue<>(16, endPointComparator);
        this.indexPoint = point;
    }

    public boolean addTuple(FrameTupleCursor cursor, TuplePointer tp) throws HyracksDataException {
        if (bufferManager.insertTuple(partition, cursor.getAccessor(), cursor.getTupleId(), tp)) {
            long point;
            if (indexPoint == 1) {
                point = IntervalJoinUtil.getIntervalEnd(cursor.getAccessor(), cursor.getTupleId(), key);
            } else {
                point = IntervalJoinUtil.getIntervalStart(cursor.getAccessor(), cursor.getTupleId(), key);
            }
            EndPointIndexItem e = new EndPointIndexItem(tp, indexPoint, point);
            indexQueue.add(e);
            active.add(tp);
            item = indexQueue.peek();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Add to memory (partition: " + partition + " index: " + e + ").");
            }
            //            System.err.println("Add to memory (partition: " + partition + " index: " + e + ").");
            return true;
        }
        return false;
    }

    public void removeTop() throws HyracksDataException {
        // Remove from active.
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Remove top from memory (partition: " + partition + " index: " + item + ").");
        }
        //        System.err.println("Remove top from memory (partition: " + partition + " index: " + item + ").");
        bufferManager.deleteTuple(partition, item.getTuplePointer());
        active.remove(item.getTuplePointer());
        indexQueue.remove(item);
        item = indexQueue.peek();
    }

    public long getTopPoint() {
        //        System.err.println("Get top from memory (partition: " + partition + " index: " + item + ").");
        return item.getPoint();
    }

    public List<TuplePointer> getActiveList() {
        return active;
    }

    public boolean isEmpty() {
        return indexQueue.isEmpty();
    }

    public boolean hasRecords() {
        return !indexQueue.isEmpty();
    }

    public void clear() throws HyracksDataException {
        for (TuplePointer leftTp : active) {
            bufferManager.deleteTuple(partition, leftTp);
        }
        indexQueue.clear();
        active.clear();
        item = null;
        bufferManager.clearPartition(partition);
    }
}
