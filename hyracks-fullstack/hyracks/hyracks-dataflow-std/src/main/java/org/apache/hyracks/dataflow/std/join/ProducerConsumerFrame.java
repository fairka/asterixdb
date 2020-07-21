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
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ProducerConsumerFrame implements IConsumerFrame {

    private final RecordDescriptor recordDescriptor;
    private ByteBuffer buffer;
    private boolean noMoreData = false;
    private boolean consumerClosed = false;
    private Lock lock = new ReentrantLock();
    private Condition frameAvailable = this.lock.newCondition();
    private Condition frameProcessed = this.lock.newCondition();

    public ProducerConsumerFrame(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    public void putFrame(ByteBuffer buffer) {
        lock.lock();
        try {
            while (this.buffer != null && !consumerClosed) {
                try {
                    frameProcessed.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (consumerClosed) {
                return;
            }
            cloneByteBuffer(buffer);
            frameAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    public void noMoreFrames() {
        lock.lock();
        try {
            noMoreData = true;
            frameAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            consumerClosed = true;
            frameProcessed.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Function copied from StackOverflow.
     * https://stackoverflow.com/questions/3366925/deep-copy-duplicate-of-javas-bytebuffer
     *
     * @param original
     */
    private void cloneByteBuffer(final ByteBuffer original) {
        // Create clone with same capacity as original.
        if (this.buffer == null || this.buffer.capacity() < original.capacity()) {
            this.buffer = (original.isDirect()) ? ByteBuffer.allocateDirect(original.capacity())
                    : ByteBuffer.allocate(original.capacity());
        }

        // Copy from the beginning
        original.rewind();
        this.buffer.put(original);
        original.rewind();
        this.buffer.flip();

        this.buffer.position(original.position());
        this.buffer.limit(original.limit());
        this.buffer.order(original.order());
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

    public boolean getFrame(IFrame returnFrame) throws HyracksDataException {
        lock.lock();
        try {
            while (this.buffer == null && !noMoreData) {
                try {
                    frameAvailable.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (this.buffer != null) {
                returnFrame.getBuffer().clear();
                if (returnFrame.getFrameSize() < buffer.capacity()) {
                    returnFrame.resize(buffer.capacity());
                }
                returnFrame.getBuffer().put(buffer.array(), 0, buffer.capacity());
                this.buffer = null;
                frameProcessed.signal();
                return true;
            } else {
                return false;
            }
        } finally {
            this.lock.unlock();
        }
    }
}
