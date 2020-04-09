/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.tsv;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class TsvRecordWriter implements RecordWriterProvider<HdfsSinkConnectorConfig> {
    private static final Logger log = LoggerFactory.getLogger(TsvRecordWriter.class);
    private static final String EXTENSION = ".tsv";
    public static final String TSV_FIELDS = "tsv.fields";
    public static final String TSV_FIELDS_DEFAULT = "";
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
    private final HdfsStorage storage;
    private final ObjectMapper mapper;
    private final JsonConverter converter;

    /**
     * 
     * Constructor.
     *
     * @param storage the underlying storage implementation.
     */
    TsvRecordWriter(HdfsStorage storage) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
        this.converter = new JsonConverter();
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {
        String tsvFields = (String) conf.get(TSV_FIELDS);

        try {
            return new RecordWriter() {
                final OutputStream out = storage.create(filename, true);
                final JsonGenerator writer = mapper.getFactory()
                        .createGenerator(out)
                        .setRootValueSeparator(null);

                @Override
                public void write(SinkRecord record) {
                    log.trace("Sink record: {}", record.toString());
                    try {
                        writer.writeObject(record.value());
                        writer.writeRaw(LINE_SEPARATOR);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                @Override
                public void commit() {}

                @Override
                public void close() {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
