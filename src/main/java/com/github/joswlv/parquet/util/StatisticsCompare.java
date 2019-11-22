package com.github.joswlv.parquet.util;

@FunctionalInterface
public interface StatisticsCompare<T, U extends Comparable<U>, Integer> {

  int compare(T statisticsCol, U keyColValue);
}
