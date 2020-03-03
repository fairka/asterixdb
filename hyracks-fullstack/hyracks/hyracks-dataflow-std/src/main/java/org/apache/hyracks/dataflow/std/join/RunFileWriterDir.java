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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;

public class RunFileWriterDir implements IFrameWriter {
    private final FileReference file;
    private final IIOManager ioManager;
    private boolean failed;

    private IFileHandle handle;
    private long size;
    private int maxOutputFrameSize;

    public RunFileWriterDir(FileReference file, IIOManager ioManager) {
        this.file = file;
        this.ioManager = ioManager;
    }

    @Override
    public void open() throws HyracksDataException {
        handle = ioManager.openDir(file, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        size = 0;
        failed = false;
        maxOutputFrameSize = 0;
    }

    @Override
    public void fail() throws HyracksDataException {
        ioManager.closeDir(handle);
        failed = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        int writen = ioManager.syncDirWrite(handle, size, buffer);
        maxOutputFrameSize = Math.max(writen, maxOutputFrameSize);
        size += writen;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!failed) {
            ioManager.closeDir(handle);
        }
    }

    public FileReference getFileReference() {
        return file;
    }

    public long getFileSize() {
        return size;
    }

    public GeneratedRunFileReaderDir createReader() throws HyracksDataException {
        flush();
        if (failed) {
            throw new HyracksDataException("createReader() called on a failed RunFileWriter");
        }
        return new GeneratedRunFileReaderDir(file, ioManager, size, false, maxOutputFrameSize);
    }

    public GeneratedRunFileReaderDir createDeleteOnCloseReader() throws HyracksDataException {
        flush();
        if (failed) {
            throw new HyracksDataException("createReader() called on a failed RunFileWriter");
        }
        return new GeneratedRunFileReaderDir(file, ioManager, size, true, maxOutputFrameSize);
    }

    @Override
    public void flush() throws HyracksDataException {
        // this is a kind of a sink operator and hence, flush() is a no op
        if (!failed) {
            ioManager.closeDir(handle);
            handle = ioManager.openDir(file, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }
    }
}
