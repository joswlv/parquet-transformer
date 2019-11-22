package com.github.joswlv.parquet.processor;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcessorTest extends TestHdfsBuilder {

  private ParquetMetaInfo parquetMetaInfo;
  private Processor processor;

  @BeforeEach
  void Before() throws IOException {
    String requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=99;100 --target-col-name=email;gender --concurrent=1 --default-fs=hdfs://localhost:11000";
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
    processor = new Processor(parquetMetaInfo, parquetMetaInfo.getPreviousFileList().get(0));
  }

  /**
   * 단일 Processor가 정상적으로 동작하는가
   */
  @Test
  void checkProcess() throws IOException {
    processor.process();

    FileStatus[] fileStatuses = dfs.listStatus(new Path(parquetMetaInfo.getOriginSourcePath()));
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(fileStatus.isFile());
      System.out.println(fileStatus.getPath().toString());
    }
  }


}
