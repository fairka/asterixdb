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
package org.apache.asterix.external.parser.test;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.converter.CSVToRecordWithMetadataAndPKConverter;
import org.apache.asterix.external.input.record.reader.stream.QuotedLineRecordReader;
import org.apache.asterix.external.input.stream.LocalFSInputStream;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.parser.RecordWithMetadataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.nontagged.AqlADMPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.junit.Assert;
import org.junit.Test;

public class RecordWithMetaTest {
    private static ARecordType recordType;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void runTest() throws Exception {
        File file = new File("target/beer.adm");
        File expected = new File(getClass().getResource("/results/beer.txt").toURI().getPath());
        try {
            FileUtils.deleteQuietly(file);
            PrintStream printStream = new PrintStream(Files.newOutputStream(Paths.get(file.toURI())));
            // create key type
            IAType[] keyTypes = { BuiltinType.ASTRING };
            String keyName = "id";
            List<String> keyNameAsList = new ArrayList<>(1);
            keyNameAsList.add(keyName);
            // create record type
            String[] recordFieldNames = {};
            IAType[] recordFieldTypes = {};
            recordType = new ARecordType("value", recordFieldNames, recordFieldTypes, true);
            // create the meta type
            String[] metaFieldNames = { keyName, "flags", "expiration", "cas", "rev", "vbid", "dtype" };
            IAType[] metaFieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT64,
                    BuiltinType.AINT32, BuiltinType.AINT32, BuiltinType.AINT32 };
            ARecordType metaType = new ARecordType("meta", metaFieldNames, metaFieldTypes, true);
            int valueIndex = 4;
            char delimiter = ',';
            int numOfTupleFields = 3;
            int[] pkIndexes = { 0 };
            int[] pkIndicators = { 1 };

            // create input stream
            LocalFSInputStream inputStream = new LocalFSInputStream(
                    new FileSplit[] { new FileSplit(null,
                            new FileReference(Paths.get(getClass().getResource("/beer.csv").toURI()).toFile())) },
                    null, null, 0, null, false);

            // create reader record reader
            QuotedLineRecordReader lineReader = new QuotedLineRecordReader(true, inputStream,
                    ExternalDataConstants.DEFAULT_QUOTE);
            // create csv with json record reader
            CSVToRecordWithMetadataAndPKConverter recordConverter = new CSVToRecordWithMetadataAndPKConverter(
                    valueIndex, delimiter, metaType, recordType, pkIndicators, pkIndexes, keyTypes);
            // create the value parser <ADM in this case>
            ADMDataParser valueParser = new ADMDataParser(recordType, false);
            // create parser.
            RecordWithMetadataParser parser = new RecordWithMetadataParser(metaType, valueParser, recordConverter);

            // create serializer deserializer and printer factories
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[keyTypes.length + 2];
            IPrinterFactory[] printerFactories = new IPrinterFactory[keyTypes.length + 2];
            for (int i = 0; i < keyTypes.length; i++) {
                serdes[i + 2] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(keyTypes[i]);
                printerFactories[i + 2] = AqlADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(keyTypes[i]);
            }
            serdes[0] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(recordType);
            serdes[1] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            printerFactories[0] = AqlADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(recordType);
            printerFactories[1] = AqlADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(metaType);
            // create output descriptor 
            IPrinter[] printers = new IPrinter[printerFactories.length];

            for (int i = 0; i < printerFactories.length; i++) {
                printers[i] = printerFactories[i].createPrinter();
            }

            ArrayTupleBuilder tb = new ArrayTupleBuilder(numOfTupleFields);
            while (lineReader.hasNext()) {
                IRawRecord<char[]> record = lineReader.next();
                tb.reset();
                parser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                parser.parseMeta(tb.getDataOutput());
                tb.addFieldEndOffset();
                parser.appendPK(tb);
                //print tuple
                printTuple(tb, printers, printStream);

            }
            lineReader.close();
            printStream.close();
            Assert.assertTrue(FileUtils.contentEquals(file, expected));
        } catch (Throwable th) {
            System.err.println("TEST FAILED");
            th.printStackTrace();
            throw th;
        } finally {
            FileUtils.deleteQuietly(file);
        }
        System.err.println("TEST PASSED.");
    }

    private void printTuple(ArrayTupleBuilder tb, IPrinter[] printers, PrintStream printStream)
            throws HyracksDataException {
        int[] offsets = tb.getFieldEndOffsets();
        for (int i = 0; i < printers.length; i++) {
            int offset = i == 0 ? 0 : offsets[i - 1];
            int length = i == 0 ? offsets[0] : offsets[i] - offsets[i - 1];
            printers[i].print(tb.getByteArray(), offset, length, printStream);
            printStream.println();
        }
    }
}
