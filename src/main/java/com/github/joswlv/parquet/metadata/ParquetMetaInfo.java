package com.github.joswlv.parquet.metadata;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

public class ParquetMetaInfo {

  //param
  private String tableFullName;
  private String keyColName;
  private List<String> keyColValueList;
  private List<String> targetColNameList;
  private int concurrent;
  private String originSourcePath;

  private boolean skipFileOption = false;
  private CompressionCodecName rewriteDefaultCompressionCodecName = CompressionCodecName.SNAPPY;
  private int rewriteDefaultBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
  private int rewriteDefaultPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
  private int rewriteDefaultDictionaryPageSize = (1024 * 2);
  private WriterVersion rewriteParquetFileWriterVersion = ParquetProperties.WriterVersion.PARQUET_1_0;

  private MessageType schema;
  private Configuration configuration;
  private List<String> previousFileList;

  public void setTableFullName(String tableFullName) {
    this.tableFullName = tableFullName;
  }

  public void setKeyColName(String keyColName) {
    this.keyColName = keyColName;
  }

  public void setKeyColValueList(List<String> keyColValueList) {
    this.keyColValueList = keyColValueList;
  }

  public void setTargetColNameList(List<String> targetColNameList) {
    this.targetColNameList = targetColNameList;
  }

  public void setConcurrent(int concurrent) {
    this.concurrent = concurrent;
  }

  public void setOriginSourcePath(String originSourcePath) {
    this.originSourcePath = originSourcePath;
  }

  public void setSkipFileOption(boolean skipFileOption) {
    this.skipFileOption = skipFileOption;
  }

  public void setRewriteDefaultCompressionCodecName(
      CompressionCodecName rewriteDefaultCompressionCodecName) {
    this.rewriteDefaultCompressionCodecName = rewriteDefaultCompressionCodecName;
  }

  public void setRewriteDefaultBlockSize(int rewriteDefaultBlockSize) {
    this.rewriteDefaultBlockSize = rewriteDefaultBlockSize;
  }

  public void setRewriteDefaultPageSize(int rewriteDefaultPageSize) {
    this.rewriteDefaultPageSize = rewriteDefaultPageSize;
  }

  public void setRewriteDefaultDictionaryPageSize(int rewriteDefaultDictionaryPageSize) {
    this.rewriteDefaultDictionaryPageSize = rewriteDefaultDictionaryPageSize;
  }

  public void setRewriteParquetFileWriterVersion(WriterVersion rewriteParquetFileWriterVersion) {
    this.rewriteParquetFileWriterVersion = rewriteParquetFileWriterVersion;
  }

  public void setSchema(MessageType schema) {
    this.schema = schema;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public void setPreviousFileList(List<String> previousFileList) {
    this.previousFileList = previousFileList;
  }

  public String getTableFullName() {
    return tableFullName;
  }

  public String getKeyColName() {
    return keyColName;
  }

  public List<String> getKeyColValueList() {
    return keyColValueList;
  }

  public List<String> getTargetColNameList() {
    return targetColNameList;
  }

  public int getConcurrent() {
    return concurrent;
  }

  public String getOriginSourcePath() {
    return originSourcePath;
  }

  public boolean isSkipFileOption() {
    return skipFileOption;
  }

  public CompressionCodecName getRewriteDefaultCompressionCodecName() {
    return rewriteDefaultCompressionCodecName;
  }

  public int getRewriteDefaultBlockSize() {
    return rewriteDefaultBlockSize;
  }

  public int getRewriteDefaultPageSize() {
    return rewriteDefaultPageSize;
  }

  public int getRewriteDefaultDictionaryPageSize() {
    return rewriteDefaultDictionaryPageSize;
  }

  public WriterVersion getRewriteParquetFileWriterVersion() {
    return rewriteParquetFileWriterVersion;
  }

  public MessageType getSchema() {
    return schema;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public List<String> getPreviousFileList() {
    return previousFileList;
  }

  @Override
  public String toString() {
    return "ParquetMetaInfo{" +
        "tableFullName='" + tableFullName + '\'' +
        ", keyColName='" + keyColName + '\'' +
        ", keyColValueList=" + keyColValueList +
        ", targetColNameList=" + targetColNameList +
        ", concurrent=" + concurrent +
        ", originSourcePath='" + originSourcePath + '\'' +
        ", skipFileOption=" + skipFileOption +
        ", rewriteDefaultCompressionCodecName=" + rewriteDefaultCompressionCodecName +
        ", rewriteDefaultBlockSize=" + rewriteDefaultBlockSize +
        ", rewriteDefaultPageSize=" + rewriteDefaultPageSize +
        ", rewriteDefaultDictionaryPageSize=" + rewriteDefaultDictionaryPageSize +
        ", rewriteParquetFileWriterVersion=" + rewriteParquetFileWriterVersion +
        ", schema=" + schema +
        ", configuration=" + configuration +
        ", previousFileList=" + previousFileList +
        '}';
  }
}