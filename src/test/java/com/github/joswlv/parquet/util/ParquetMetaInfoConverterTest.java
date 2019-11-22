package com.github.joswlv.parquet.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.util.Arrays;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetMetaInfoConverterTest extends TestHdfsBuilder {

  private ParquetMetaInfoConverter parquetMetaInfoConverter;
  private ParquetMetaInfo parquetMetaInfo;
  private String requiredArg;

  @BeforeAll
  void Before() {
    parquetMetaInfoConverter = new ParquetMetaInfoConverter();
    requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=-1;1002 --target-col-name=email;gender --default-fs=hdfs://localhost:11000";
  }

  /**
   * 꼭 필요한 MetaInfo을 정상적으로 처리하는가
   */
  @Test
  void checkRequiredMetaInfo() {
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" "));

    assertEquals(parquetMetaInfo.getTableFullName(), "edw.sample");
    assertEquals(parquetMetaInfo.getKeyColName(), "id");
    assertEquals(parquetMetaInfo.getKeyColValueList(), Arrays.asList("-1;1002".split(";")));
    assertEquals(parquetMetaInfo.getTargetColNameList(), Arrays.asList("email;gender".split(";")));

  }

  @Test
  void 파일개수로_concurrent값_설정() {
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" "));
    assertEquals(parquetMetaInfo.getConcurrent(), 5);
  }

  @Test
  void 입력으로_concurrent값_설정() {
    String overwriteArg = requiredArg + " --concurrent=2";
    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" "));
    assertEquals(parquetMetaInfo.getConcurrent(), 2);
  }

  /**
   * Optional한 MetaInfo가 입력없을시, 정상적으로 Default값으로 세팅이 되는가 getRewriteDefaultBlockSize
   */
  @Test
  void checkDefaultBlockSizeMetaInfo() {
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" "));

    assertEquals(parquetMetaInfo.getRewriteDefaultBlockSize(), ParquetWriter.DEFAULT_BLOCK_SIZE);
  }

  /**
   * Optional한 MetaInfo가 입력이 있을시, 정상적으로 입력한 값으로 세팅이 되는가 getRewriteDefaultBlockSize
   */
  @Test
  void checkOverwriteDefaultBlockSizeMetaInfo() {
    String overwriteArg = requiredArg + " --block-size=1024";
    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" "));

    assertEquals(parquetMetaInfo.getRewriteDefaultBlockSize(), 1024);
  }


  /**
   * Optional한 MetaInfo가 입력없을시, 정상적으로 Default값으로 세팅이 되는가 getRewriteDefaultCompressionCodecName
   */
  @Test
  void checkDefaultCompressionCodecName() {
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" "));

    assertEquals(parquetMetaInfo.getRewriteDefaultCompressionCodecName(),
        CompressionCodecName.SNAPPY);

  }

  /**
   * Optional한 MetaInfo가 입력이 있을시, 정상적으로 입력한 값으로 세팅이 되는가 getRewriteDefaultCompressionCodecName
   */
  @Test
  void checkOverwriteCompressionCodecName() {
    String overwriteArg = requiredArg + " --compression=LZO";
    parquetMetaInfo = ParquetMetaInfoConverter.build(overwriteArg.split(" "));

    assertEquals(parquetMetaInfo.getRewriteDefaultCompressionCodecName(), CompressionCodecName.LZO);
  }

  /**
   * Parquet 폴더로 부터, 읽어올 Parquet File을 모두 읽어올 수 있는가?
   */
  @Test
  void checkPreviousFileList() {
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
    parquetMetaInfo.setConfiguration(conf);

    for (String path : parquetMetaInfo.getPreviousFileList()) {
      System.out.println(path);
    }
    assertEquals(parquetMetaInfo.getPreviousFileList().size(), 5);
  }

  /**
   * Parquet 파일로 부터, Schema정보를 정상적으로 읽어올수있는가?
   */
  @Test
  void checkSetSchema() {

    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" "));
    System.out.println(parquetMetaInfo.getSchema().toString());

  }

}