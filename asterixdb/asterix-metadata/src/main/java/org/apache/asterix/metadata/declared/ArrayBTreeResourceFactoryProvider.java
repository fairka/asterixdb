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
package org.apache.asterix.metadata.declared;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.FilesIndexDescription;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;

public class ArrayBTreeResourceFactoryProvider implements IResourceFactoryProvider {

    public static final ArrayBTreeResourceFactoryProvider INSTANCE = new ArrayBTreeResourceFactoryProvider();

    private ArrayBTreeResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        int[] filterFields = IndexUtil.getFilterFields(dataset, index, filterTypeTraits);
        int[] btreeFields = IndexUtil.getBtreeFieldsIfFiltered(dataset, index);
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories = getCmpFactories(mdProvider, dataset, index, recordType, metaType);
        int[] bloomFilterFields = getBloomFilterFields(dataset, index);
        double bloomFilterFalsePositiveRate = mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        boolean hasBloomFilter = bloomFilterFields != null;
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                return index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))
                        ? new ExternalBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories,
                                filterTypeTraits, filterCmpFactories, filterFields, opTrackerFactory,
                                ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                                ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                                bloomFilterFalsePositiveRate, false, btreeFields, hasBloomFilter)
                        : new ExternalBTreeWithBuddyLocalResourceFactory(storageManager, typeTraits, cmpFactories,
                                filterTypeTraits, filterCmpFactories, filterFields, opTrackerFactory,
                                ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                                ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                                bloomFilterFalsePositiveRate, false, btreeFields, hasBloomFilter);
            case INTERNAL:
                AsterixVirtualBufferCacheProvider vbcProvider =
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());

                final ICompressorDecompressorFactory compDecompFactory;
                if (index.isPrimaryIndex()) {
                    //Compress only primary index
                    compDecompFactory = mdProvider.getCompressionManager().getFactory(dataset.getCompressionScheme());
                } else {
                    compDecompFactory = NoOpCompressorDecompressorFactory.INSTANCE;
                }

                return new LSMBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                        filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory,
                        pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                        mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                        bloomFilterFalsePositiveRate, index.isPrimaryIndex(), btreeFields, compDecompFactory,
                        hasBloomFilter);
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                        dataset.getDatasetType().toString());
        }
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return primaryTypeTraits;
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS;
        }
        ITypeTraitProvider typeTraitProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        ITypeTraits[] secondaryTypeTraits;
        Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
        int numSecondaryKeys =
                arrayIndexDetails.getElementList().stream().map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        int secondaryTypeTraitPos = 0;
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            ARecordType sourceType;
            if (e.getSourceIndicator() == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            for (int i = 0; i < e.getProjectList().size(); i++) {
                List<String> project = e.getProjectList().get(i);
                Pair<IAType, Boolean> keyTypePair = ArrayIndexUtil.getNonNullableOpenFieldType(e.getTypeList().get(i),
                        ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project), sourceType,
                        ArrayIndexUtil.getArrayDepthIndicator(e.getUnnestList(), project));
                IAType keyType = keyTypePair.first;
                secondaryTypeTraits[secondaryTypeTraitPos++] = typeTraitProvider.getTypeTrait(keyType);
            }
        }
        // Add serializers and comparators for primary index fields.
        System.arraycopy(primaryTypeTraits, 0, secondaryTypeTraits, numSecondaryKeys, numPrimaryKeys);
        return secondaryTypeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactory[] primaryCmpFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.FILES_INDEX_COMP_FACTORIES;
        }
        Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
        int numSecondaryKeys =
                arrayIndexDetails.getElementList().stream().map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        IBinaryComparatorFactory[] secondaryCmpFactories =
                new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        int secondaryCmpFactoriesPos = 0;
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            ARecordType sourceType;
            if (e.getSourceIndicator() == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            for (int i = 0; i < e.getProjectList().size(); i++) {
                List<String> project = e.getProjectList().get(i);
                Pair<IAType, Boolean> keyTypePair = ArrayIndexUtil.getNonNullableOpenFieldType(e.getTypeList().get(i),
                        ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project), sourceType,
                        ArrayIndexUtil.getArrayDepthIndicator(e.getUnnestList(), project));
                IAType keyType = keyTypePair.first;
                secondaryCmpFactories[secondaryCmpFactoriesPos++] =
                        cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
            }
        }
        // Add serializers and comparators for primary index fields.
        System.arraycopy(primaryCmpFactories, 0, secondaryCmpFactories, numSecondaryKeys, numPrimaryKeys);
        return secondaryCmpFactories;
    }

    private static int[] getBloomFilterFields(Dataset dataset, Index index) throws AlgebricksException {
        // both the Primary index and the Primary Key index have bloom filters
        if (index.isPrimaryIndex() || index.isPrimaryKeyIndex()) {
            return dataset.getPrimaryBloomFilterFields();
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            if (index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
                return FilesIndexDescription.BLOOM_FILTER_FIELDS;
            } else {
                Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                return new int[] { arrayIndexDetails.getElementList().stream().map(e -> e.getProjectList().size())
                        .reduce(0, Integer::sum) };
            }
        }
        // secondary btrees and rtrees do not have bloom filters
        return null;
    }
}
