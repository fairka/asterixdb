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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * General Frame based buffer manager class
 */
public class FrameBufferManager implements IFrameBufferManager {

    ArrayList<ByteBuffer> buffers = new ArrayList<>();

    @Override
    public void reset() throws HyracksDataException {
        buffers.clear();
    }

    @Override
    public BufferInfo getFrame(int frameIndex, BufferInfo returnedInfo) {
        returnedInfo.reset(buffers.get(frameIndex), 0, buffers.get(frameIndex).capacity());
        return returnedInfo;
    }

    @Override
    public int getNumFrames() {
        return buffers.size();
    }

    @Override
    public int insertFrame(ByteBuffer frame) throws HyracksDataException {
        buffers.add(frame);
        return buffers.size() - 1;
    }

    @Override
    public void close() {
        buffers = null;
    }

    @Override
    public int next() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean exists() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void resetIterator() {
        // TODO Auto-generated method stub

    }

    @Override
    public ITupleAccessor getTupleAccessor(RecordDescriptor rd) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeFrame(int frameIndex) {
    }

}
