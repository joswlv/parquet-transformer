package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

public class GracefulWriter extends IoProcessor {

  private ParquetWriter<Group> writer;

  public GracefulWriter(ParquetMetaInfo metaInfo, String parquetWriteFilePath) {

    Path filePath = new Path(parquetWriteFilePath);
    Configuration conf = metaInfo.getConfiguration();

    try {
      DistributedFileSystem dfs = (DistributedFileSystem) DistributedFileSystem
          .newInstance(conf);
      if (dfs.exists(filePath)) {
        dfs.delete(filePath, true);
      }

      GroupWriteSupport.setSchema(metaInfo.getSchema(), conf);
      GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
      writer = new ParquetWriter<>(filePath,
          groupWriteSupport,
          metaInfo.getRewriteDefaultCompressionCodecName(),
          metaInfo.getRewriteDefaultBlockSize(),
          metaInfo.getRewriteDefaultPageSize(),
          metaInfo.getRewriteDefaultDictionaryPageSize(),
          true,
          false,
          metaInfo.getRewriteParquetFileWriterVersion(),
          conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void write(Group record) {
    try {
      writer.write(record);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
