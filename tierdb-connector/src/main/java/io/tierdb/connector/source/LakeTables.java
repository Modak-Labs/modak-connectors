package io.tierdb.connector.source;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

public final class LakeTables {

    private LakeTables() {}

    public static FileIO fileIo(Map<String, String> properties) {
        ResolvingFileIO io = new ResolvingFileIO();
        io.setConf(new Configuration());
        io.initialize(properties);
        return io;
    }
}
