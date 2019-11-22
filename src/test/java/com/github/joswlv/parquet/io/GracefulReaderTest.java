package com.github.joswlv.parquet.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GracefulReaderTest extends TestHdfsBuilder {

  private ParquetMetaInfo parquetMetaInfo;
  private GracefulReader gracefulReader;


  @BeforeEach
  void Before() throws IOException {
    String requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=99;100 --target-col-name=email;gender --concurrent=3 --default-fs=hdfs://localhost:11000";
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
    gracefulReader = new GracefulReader(parquetMetaInfo,
        parquetMetaInfo.getPreviousFileList().get(0));

  }

  /**
   * 모든 ParquetFile을 에러없이 읽을 수 있는가?
   */
  @Test
  void checkReadAllRow() throws IOException {
    assertEquals(gracefulReader.getData().count(), 1000);
  }

  /**
   * Row가 정상적으로 읽히는가? (디버깅용도)
   */
  @Test
  void showRowContentForDebug() throws IOException {
    gracefulReader.getData().forEach(record -> {
      System.out.println(record.toString());
    });
  }

}