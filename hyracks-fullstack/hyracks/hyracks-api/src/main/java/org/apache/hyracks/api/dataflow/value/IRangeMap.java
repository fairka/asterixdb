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
package org.apache.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface IRangeMap extends Serializable {

    public int getSplitCount();

    public int getMinSlotFromPartition(int partition, int nPartitions);

    public int getMaxSlotFromPartition(int partition, int nPartitions);

    public int getPartitionFromSlot(int slot, int nPartitions);

    public byte[] getByteArray(int columnIndex, int splitIndex);

    public int getStartOffset(int columnIndex, int splitIndex);

    public int getLength(int columnIndex, int splitIndex);

    public byte getTag(int columnIndex, int splitIndex);

    // Min value functions
    public byte[] getMinByteArray(int columnIndex);

    public int getMinStartOffset(int columnIndex);

    public int getMinLength(int columnIndex);

    public byte getMinTag(int columnIndex);

    // Max value functions
    public byte[] getMaxByteArray(int columnIndex);

    public int getMaxStartOffset(int columnIndex);

    public int getMaxLength(int columnIndex);

    public byte getMaxTag(int columnIndex);
}
