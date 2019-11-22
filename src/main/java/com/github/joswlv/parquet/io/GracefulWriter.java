package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulWriter implements IoProcessor {

  private Logger log = LoggerFactory.getLogger(this.getClass());

  private ParquetWriter<Group> writer;

  public GracefulWriter(ParquetMetaInfo metaInfo, String targetParquetFilePath) throws IOException {

    Path filePath = new Path(targetParquetFilePath + "_tmp");
    Configuration conf = metaInfo.getConfiguration();

    GroupWriteSupport.setSchema(metaInfo.getSchema(), conf);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    writer = new ParquetWriter<>(filePath,
        groupWriteSupport,
        metaInfo.getRewriteDefaultCompressionCodecName(),
        metaInfo.getRewriteDefaultBlockSize(),
        metaInfo.getRewriteDefaultPageSize(),
        metaInfo.getRewriteDefaultDictionaryPageSize(),
        false,
        false,
        metaInfo.getRewriteParquetFileWriterVersion(),
        conf);
  }

  public void write(Group record) {
    try {
      writer.write(record);
    } catch (IOException e) {
      log.error("record write error!, ", e.getMessage(), e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}