package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

public class GracefulReader extends IoProcessor {

  private Configuration conf;
  private MessageType schema;
  private ParquetFileReader fileReader;

  private Group group;

  public GracefulReader(ParquetMetaInfo metaInfo, String targetParquetFilePath) throws IOException {
    this.conf = metaInfo.getConfiguration();
    this.schema = metaInfo.getSchema();
    this.fileReader = ParquetFileReader.open(conf, new Path(targetParquetFilePath));
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
    PageReadStore pageReadStore;
    try {
      if ((pageReadStore = fileReader.readNextRowGroup()) != null) {
        MessageColumnIO messageColumnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = messageColumnIO
            .getRecordReader(pageReadStore, new GroupRecordConverter(schema));

        if ((group = (Group) recordReader.read()) != null) {
          return true;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void close() {
    try {
      if (fileReader != null) {
        fileReader.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
