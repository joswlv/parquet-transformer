package com.github.joswlv.parquet.transform;

@FunctionalInterface
public interface Transform<S, T> {

  T transform(S data);
}
