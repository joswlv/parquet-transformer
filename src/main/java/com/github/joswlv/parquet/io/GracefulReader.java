package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulReader implements IoProcessor {

  private Logger log = LoggerFactory.getLogger(this.getClass());

  private ParquetReader<Group> fileReader;

  private Group group;

  public GracefulReader(ParquetMetaInfo metaInfo, String targetParquetFilePath) throws IOException {
    this.fileReader = ParquetReader.builder(new GroupReadSupport(), new Path(targetParquetFilePath))
        .withConf(metaInfo.getConfiguration()).build();
  }

  public Stream<Group> getData() {
    return StreamSupport
        .stream(new Spliterators.AbstractSpliterator<Group>(Long.MAX_VALUE, Spliterator.ORDERED) {

          @Override
          public boolean tryAdvance(Consumer<? super Group> action) {
            if (hasNextData()) {
              action.accept(group);
              return true;
            } else {
              return false;
            }
          }
        }, false);
  }

  private boolean hasNextData() {
    try {
      if ((group = fileReader.read()) != null) {
        return true;
      }

    } catch (IOException e) {
      log.error("record read error!, ", e.getMessage(), e);
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    fileReader.close();
  }
}