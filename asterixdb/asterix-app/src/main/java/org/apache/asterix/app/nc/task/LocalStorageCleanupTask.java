/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the LocalStorageCleanupTask at
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
package org.apache.asterix.app.nc.task;

import java.util.Set;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;

public class LocalStorageCleanupTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private final int metadataPartitionId;

    public LocalStorageCleanupTask(int metadataPartitionId) {
        this.metadataPartitionId = metadataPartitionId;
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appContext.getLocalResourceRepository();
        deleteInvalidMetadataIndexes(localResourceRepository);
        final Set<Integer> nodePartitions = appContext.getReplicaManager().getPartitions();
        localResourceRepository.deleteCorruptedResources();
        //TODO optimize this to cleanup all active partitions at once
        for (Integer partition : nodePartitions) {
            localResourceRepository.cleanup(partition);
        }
    }

    private void deleteInvalidMetadataIndexes(PersistentLocalResourceRepository localResourceRepository)
            throws HyracksDataException {
        localResourceRepository.deleteInvalidIndexes(r -> {
            DatasetLocalResource lr = (DatasetLocalResource) r.getResource();
            return MetadataIndexImmutableProperties.isMetadataDataset(lr.getDatasetId())
                    && lr.getPartition() != metadataPartitionId;
        });
    }

    @Override
    public String toString() {
        return "LocalStorageCleanupTask{" + "metadataPartitionId=" + metadataPartitionId + '}';
    }
}
