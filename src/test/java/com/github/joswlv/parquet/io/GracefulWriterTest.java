package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GracefulWriterTest extends TestHdfsBuilder {

  private ParquetMetaInfo parquetMetaInfo;
  private GracefulReader gracefulReader;
  private GracefulWriter gracefulWriter;


  @BeforeAll
  void Before() throws IOException {
    String requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=99;100 --target-col-name=email;gender --concurrent=3 --default-fs=hdfs://localhost:11000";
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
    gracefulReader = new GracefulReader(parquetMetaInfo,
        parquetMetaInfo.getPreviousFileList().get(0));
    gracefulWriter = new GracefulWriter(parquetMetaInfo,
        parquetMetaInfo.getPreviousFileList().get(0));

  }

  /**
   * Parquet File이 정상적으로 Write되는가
   */
  @Test
  void writeTest() throws IOException {
    gracefulReader.getData().forEach(group -> {
      gracefulWriter.write(group);
    });
    gracefulWriter.close();

    FileStatus[] fileStatuses = dfs.listStatus(new Path(parquetMetaInfo.getOriginSourcePath()));
    System.out.println("SIZE : " + fileStatuses.length);
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(fileStatus.isFile());
      System.out.println(fileStatus.getPath().toString());
    }

  }


}