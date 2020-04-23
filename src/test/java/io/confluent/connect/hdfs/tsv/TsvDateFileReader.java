package io.confluent.connect.hdfs.tsv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.confluent.connect.hdfs.DataFileReader;

public class TsvDateFileReader implements DataFileReader {

    @Override
    public Collection<Object> readData(Configuration conf, Path path) throws IOException {
        String uri = "hdfs://127.0.0.1:9001";
        try (FileSystem fs = FileSystem.newInstance(new URI(uri), conf)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {

                ArrayList<Object> records = new ArrayList<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                    records.add(line);
                }
                return records;
            }
        } catch (URISyntaxException e) {
            throw new IOException("Failed to create URI: " + uri);
        }
    }
}
