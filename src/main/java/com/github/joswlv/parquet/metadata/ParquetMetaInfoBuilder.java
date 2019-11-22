package com.github.joswlv.parquet.metadata;

import java.util.List;
import org.apache.hadoop.conf.Configuration;

public final class ParquetMetaInfoBuilder {

  //param
  private String tableFullName;
  private String keyColName;
  private List<String> keyColValueList;
  private List<String> targetColNameList;
  private int concurrent;
  private Configuration configuration;

  public ParquetMetaInfoBuilder() {
  }

  public static ParquetMetaInfoBuilder builder() {
    return new ParquetMetaInfoBuilder();
  }

  public ParquetMetaInfoBuilder tableFullName(String tableFullName) {
    this.tableFullName = tableFullName;
    return this;
  }

  public ParquetMetaInfoBuilder keyColName(String keyColName) {
    this.keyColName = keyColName;
    return this;
  }

  public ParquetMetaInfoBuilder keyColValueList(List<String> keyColValueList) {
    this.keyColValueList = keyColValueList;
    return this;
  }

  public ParquetMetaInfoBuilder targetColNameList(List<String> targetColNameList) {
    this.targetColNameList = targetColNameList;
    return this;
  }

  public ParquetMetaInfoBuilder concurrent(int concurrent) {
    this.concurrent = concurrent;
    return this;
  }

  public ParquetMetaInfoBuilder configuration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  public ParquetMetaInfo build() {
    ParquetMetaInfo parquetMetaInfo = new ParquetMetaInfo();
    parquetMetaInfo.setTableFullName(tableFullName);
    parquetMetaInfo.setKeyColName(keyColName);
    parquetMetaInfo.setKeyColValueList(keyColValueList);
    parquetMetaInfo.setTargetColNameList(targetColNameList);
    parquetMetaInfo.setConcurrent(concurrent);
    parquetMetaInfo.setConfiguration(configuration);
    return parquetMetaInfo;
  }
}
