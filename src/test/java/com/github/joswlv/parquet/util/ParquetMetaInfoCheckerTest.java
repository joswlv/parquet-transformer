package com.github.joswlv.parquet.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetMetaInfoCheckerTest extends TestHdfsBuilder {

  private ParquetMetaInfoConverter parquetMetaInfoConverter;
  private ParquetMetaInfo parquetMetaInfo;
  private String requiredArg;

  @BeforeAll
  void Before() {
    parquetMetaInfoConverter = new ParquetMetaInfoConverter();
    requiredArg = "--table-name=edw.sample --key-col-name=id  --target-col-name=email;gender --concurrent=3 --default-fs=hdfs://localhost:11000";
  }

  /**
   * skipFileOption을 설정하지 않을 경우(default), 모든 ParquetFile을 다 읽는다
   */
  @Test
  void checkSetSkipFileOptionFalse() {
    String overwriteArg = requiredArg + " --key-col-value=-1";

    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" ", -1));
    assertEquals(parquetMetaInfo.isSkipFileOption(), false);
    assertEquals(parquetMetaInfo.getPreviousFileList().size(), 5);
  }

  /**
   * skipFileOption을 설정하고, key-col-value이 필드에 포함되어있는 파일을 추가한다
   */
  @Test
  void checkSetSkipFileOptionTrueOneColValue() {
    String overwriteArg = requiredArg + " --key-col-value=100 --skip-file-option=true";

    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" ", -1));
    assertEquals(parquetMetaInfo.isSkipFileOption(), true);
    assertEquals(parquetMetaInfo.getPreviousFileList().size(), 1);
    assertEquals(parquetMetaInfo.getPreviousFileList().get(0),
        "hdfs://localhost:11000/user/hive/warehouse/edw.db/sample/userdata1.parquet");

  }


  /**
   * skipFileOption을 설정하고, key-col-value 중, 하나라도 포함된 파일은 모두 추가한다 100, 101이 포함된 userdata1.parquet만
   * 읽음
   */
  @Test
  void checkSetSkipFileOptionTrueMultiColValueOneFile() {
    String overwriteArg = requiredArg + " --key-col-value=-1;100;101 --skip-file-option=true";

    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" ", -1));
    assertEquals(parquetMetaInfo.isSkipFileOption(), true);
    assertEquals(parquetMetaInfo.getPreviousFileList().size(), 1);
    assertEquals(parquetMetaInfo.getPreviousFileList().get(0),
        "hdfs://localhost:11000/user/hive/warehouse/edw.db/sample/userdata1.parquet");

  }

  /**
   * skipFileOption을 설정하고, key-col-value 중, 하나라도 포함된 파일은 모두 추가한다 100이 포함된 userdata1.parquet와 3000이
   * 포함된 userdata3.parquet, 2개 파일 읽음
   */
  @Test
  void checkSetSkipFileOptionTrueMultiColValueTwoFile() {
    String overwriteArg = requiredArg + " --key-col-value=-1;100;3002 --skip-file-option=true";

    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" ", -1));
    assertEquals(parquetMetaInfo.isSkipFileOption(), true);
    assertEquals(parquetMetaInfo.getPreviousFileList().size(), 2);

  }

  /**
   * skipFileOption을 설정하고, key-col-value가 모두 포함되지 않는다면, 파일을 추가하지 않는다.
   */
  @Test
  void checkSetSkipFileOptionTrueMultiColValueNotContain() {
    String overwriteArg = requiredArg + " --key-col-value=-1;-100 --skip-file-option=true";

    assertThrows(RuntimeException.class, () ->
        parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" ", -1)));
  }


}