package com.github.joswlv.parquet.util;

import org.apache.parquet.column.statistics.Statistics;

public class MaxStatisticsCompare<U extends Comparable<U>> implements
    StatisticsCompare<Statistics, U, Integer> {

  @Override
  public int compare(Statistics statisticsCol, U keyColValue) {
    return statisticsCol.compareMaxToValue(keyColValue);
  }
}
