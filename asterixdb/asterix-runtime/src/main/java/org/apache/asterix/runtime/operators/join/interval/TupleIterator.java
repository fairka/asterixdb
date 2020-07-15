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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.AbstractTupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class TupleIterator {

    IFrameBufferManager bufferManager;

    public TupleIterator(IFrameBufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    /**
     * Create a iterator for frames.
     *
     * Allows the reuse of frame ids.
     */
    int next(){
        return 0;
    }

    boolean exists(){
        return false;
    }

    void resetIterator(){

    }

    ITupleAccessor getTupleAccessor(RecordDescriptor rd){
        return null;
    }

//From VPartitionTupleBufferManager
//    @Override
//    public int next() {
//        while (++iterator < buffers.size()) {
//            if (buffers.get(iterator) != null) {
//                break;
//            }
//        }
//        return iterator;
//    }
//
//    @Override
//    public boolean exists() {
//        return iterator < buffers.size() && buffers.get(iterator) != null;
//    }
//
//    @Override
//    public void resetIterator() {
//        iterator = -1;
//    }
//
//    @Override
//    public ITupleAccessor getTupleAccessor(final RecordDescriptor recordDescriptor) {
//        return new AbstractTupleAccessor() {
//            protected BufferInfo tempBI = new BufferInfo(null, -1, -1);
//            FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);
//
//            @Override IFrameTupleAccessor getInnerAccessor() {
//                return innerAccessor;
//            }
//
//            @Override
//            void resetInnerAccessor(int frameIndex) {
//                getFrame(frameIndex, tempBI);
//                innerAccessor.reset(tempBI.getBuffer(), tempBI.getStartOffset(), tempBI.getLength());
//            }
//
//            void resetInnerAccessor(TuplePointer tuplePointer) {
//                getFrame(tuplePointer.getFrameIndex(), tempBI);
//                innerAccessor.reset(tempBI.getBuffer(), tempBI.getStartOffset(), tempBI.getLength());
//            }
//
//            @Override
//            int getFrameCount() {
//                return buffers.size();
//            }
//        };
//    }
//@Override
//public ITupleAccessor getTupleAccessor(final RecordDescriptor recordDescriptor) {
//    return new AbstractTupleAccessor() {
//        FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);
//
//        @Override
//        IFrameTupleAccessor getInnerAccessor() {
//            return innerAccessor;
//        }
//
//        @Override
//        void resetInnerAccessor(TuplePointer tuplePointer) {
//            partitionArray[parsePartitionId(tuplePointer.getFrameIndex())]
//                    .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
//            innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
//        }
//
//        @Override
//        void resetInnerAccessor(int frameIndex) {
//            partitionArray[parsePartitionId(frameIndex)].getFrame(parseFrameIdInPartition(frameIndex), tempInfo);
//            innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
//        }
//
//        @Override
//        int getFrameCount() {
//            return partitionArray.length;
//        }
//    };
//}
}
