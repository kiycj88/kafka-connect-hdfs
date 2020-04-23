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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class TsvRecordWriter implements RecordWriterProvider<HdfsSinkConnectorConfig> {
    private static final Logger log = LoggerFactory.getLogger(TsvRecordWriter.class);
    private static final String EXTENSION = ".tsv";
    public static final String TSV_SCHEMA = "tsvSchema";
    public static final String TIME_STAMP_FIELDS = "timeStampFields";
    private static final int WRITER_BUFFER_SIZE = 128 * 1024;
    private static final String TAB_DELIMITER = "\t";
    public static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("Asia/Tokyo");

    public static final String RECORD_PARTITION_FIELD = "record.partition";
    public static final String RECORD_OFFSET_FIELD = "record.offset";
    private final HdfsStorage storage;
    private final ObjectMapper mapper;

    /**
     * 
     * Constructor.
     *
     * @param storage the underlying storage implementation.
     */
    TsvRecordWriter(HdfsStorage storage) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {
        return new RecordWriter() {
            final OutputStream out = storage.create(filename, true);
            final OutputStreamWriter streamWriter = new OutputStreamWriter(out, Charset.defaultCharset());
            final BufferedWriter writer = new BufferedWriter(streamWriter, WRITER_BUFFER_SIZE);

            @Override
            public void write(SinkRecord record) {
                log.trace("Sink record: {}", record.toString());
                try {
                    Object value = record.value();
                    JsonNode jsonNode = mapper.valueToTree(value);
                    JsonNode tsvSchemaNode = jsonNode.get(TSV_SCHEMA);
                    if (tsvSchemaNode == null || StringUtils.isEmpty(tsvSchemaNode.asText())) {
                        throw new ConnectException("'tsvSchema' is mandatory for TsvFormat");
                    }
                    List<String> tsvFields = Arrays.stream(tsvSchemaNode.asText().split(","))
                            .map(tsvField -> tsvField.trim())
                            .collect(toList());

                    JsonNode timeStampFieldsNode = jsonNode.get(TIME_STAMP_FIELDS);
                    final Set<String> timeStampFields = timeStampFieldsNode == null
                            ? Collections.emptySet()
                            : Arrays.stream(timeStampFieldsNode.asText().split(","))
                                    .map(timeStampField -> timeStampField.trim())
                                    .collect(toSet());

                    String timezoneInConf = conf.getString("timezone");
                    final ZoneId zoneId = StringUtils.isEmpty(timezoneInConf) ? DEFAULT_ZONE_ID : ZoneId.of(timezoneInConf);

                    String tsvValue = tsvFields.stream()
                            .map(fieldName -> {
                                if (RECORD_PARTITION_FIELD.equalsIgnoreCase(fieldName)) {
                                    return String.valueOf(record.kafkaPartition());
                                } else if (RECORD_OFFSET_FIELD.equalsIgnoreCase(fieldName)) {
                                    return String.valueOf(record.kafkaOffset());
                                }
                                String jsonPointer = JsonPointer.SEPARATOR + fieldName.replace('.', JsonPointer.SEPARATOR);
                                JsonNode fieldValue = jsonNode.at(jsonPointer);

                                String result = fieldValue == null ? "" : fieldValue.asText("");

                                if (timeStampFields.contains(fieldName)) {
                                    try {
                                        long time = Long.valueOf(result).longValue();
                                        return Instant.ofEpochMilli(time)
                                                .atZone(zoneId)
                                                .format(DEFAULT_DATETIME_FORMATTER);
                                    } catch (NumberFormatException ex) {
                                        log.warn("number format exception {},{}", fieldName, fieldValue);
                                        return result;
                                    }
                                }

                                return result;

                            })
                            .collect(joining(TAB_DELIMITER));
                    writer.write(tsvValue);
                    writer.newLine();
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
    }

}
