package io.confluent.connect.hdfs.tsv;

import static io.confluent.connect.hdfs.tsv.TsvRecordWriter.RECORD_OFFSET_FIELD;
import static io.confluent.connect.hdfs.tsv.TsvRecordWriter.RECORD_PARTITION_FIELD;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;

public class DataWriterTsvTest extends TestWithMiniDFSCluster {

    protected final ObjectMapper mapper = new ObjectMapper();
    private String configTsvFields = "a,b,f,c,d,e";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataFileReader = new TsvDateFileReader();
        extension = ".tsv";
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, TsvFormat.class.getName());
        return props;
    }

    @Test
    public void testNoSchema() throws Exception {
        DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
        partitioner = hdfsWriter.getPartitioner();
        hdfsWriter.recover(TOPIC_PARTITION);

        List<SinkRecord> sinkRecords = createRecordsWithoutSchema(
                7 * context.assignment().size(),
                context.assignment());

        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
        hdfsWriter.stop();

        // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
        long[] validOffsets = { 0, 3, 6 };
        verify(sinkRecords, validOffsets, context.assignment());
    }

    protected List<SinkRecord> createRecordsWithoutSchema(
            int size,
            Set<TopicPartition> partitions) {
        String key = "key";

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0, total = 0; total < size; ++ offset) {
            for (TopicPartition tp : partitions) {
                Map<String, Object> map = new HashMap<>();
                map.put("tsvSchema", configTsvFields);
                map.put("a", "test-" + offset);
                map.put("b", true);
                map.put("c", null);
                map.put("d", offset);
                map.put("e", offset + 0.1);
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, key, null, map, offset));
                if (++ total >= size) {
                    break;
                }
            }
        }
        return sinkRecords;
    }

    @Override
    protected void verifyContents(
            List<SinkRecord> expectedRecords,
            int startIndex,
            Collection<Object> records) {
        for (Object tsvRecord : records) {
            SinkRecord expectedRecord = expectedRecords.get(2 * startIndex ++);
            Object expectedValue = expectedRecord.value();

            JsonNode jsonNode = mapper.valueToTree(expectedValue);
            expectedValue = Arrays.stream(configTsvFields.split(","))
                    .map(fieldName -> {
                        if (RECORD_PARTITION_FIELD.equalsIgnoreCase(fieldName)) {
                            return String.valueOf(expectedRecord.kafkaPartition());
                        } else if (RECORD_OFFSET_FIELD.equalsIgnoreCase(fieldName)) {
                            return String.valueOf(expectedRecord.kafkaOffset());
                        }
                        JsonNode fieldValue = jsonNode.get(fieldName);
                        return fieldValue == null ? "" : fieldValue.asText("");
                    })
                    .collect(joining("\t"));

            assertEquals(expectedValue, tsvRecord);
        }
    }
}
