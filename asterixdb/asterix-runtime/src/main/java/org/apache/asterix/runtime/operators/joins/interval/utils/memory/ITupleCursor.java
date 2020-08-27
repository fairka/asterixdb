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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;

/**
 * Represents an index cursor. The expected use
 * cursor = new cursor();
 * while(predicate){
 * -cursor.reset()
 * -cursor.next()
 * -while (cursor.exists()){
 * --cursor.next()
 * -}
 * }
 *
 * next() is used instead of hasNext() because there
 * is a case when a cursor.hasNext() may need to look
 * at the next frame to determine if the tuple exists.
 * cursor.exists() ensures we don't need to load the
 * next frame when checking.
 */
public interface ITupleCursor<T> {

    /**
     * Checks if the Current Tuple Index Exists
     *
     * @return
     */
    boolean exists();

    /**
     * Increments the Tuple Index
     *
     */
    void next();

    /**
     * Get the Current Tuple Index
     *
     * @return
     */
    int getTupleId();

    /**
     * Set the Tuple Index
     * @param tupleId
     */
    void setTupleId(int tupleId);

    /**
     * Used in FrameTupleCursor to reset the accessor to the buffer
     *
     * @param param
     */
    void reset(T param);

    /**
     * Return the accessor
     *
     * @return
     */
    IFrameTupleAccessor getAccessor();
}
