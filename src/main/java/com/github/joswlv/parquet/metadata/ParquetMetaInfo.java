package com.github.joswlv.parquet.metadata;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

@Getter
@Setter
@Builder
@ToString
public class ParquetMetaInfo {

  //param
  private String tableFullName;
  private String keyColName;
  private List<String> keyColValueList;
  private List<String> targetColNameList;
  private int concurrent;
  private String originSourcePath;
  private CompressionCodecName rewriteDefaultCompressionCodecName = CompressionCodecName.SNAPPY;
  private int rewriteDefaultBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
  private int rewriteDefaultPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
  private int rewriteDefaultDictionaryPageSize = (1024 * 2);
  private WriterVersion rewriteParquetFileWriterVersion = ParquetProperties.WriterVersion.PARQUET_1_0;

  private MessageType schema;
  private Configuration configuration;
  private List<String> previousFileList;

}