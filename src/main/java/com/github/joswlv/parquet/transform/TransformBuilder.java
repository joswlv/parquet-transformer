package com.github.joswlv.parquet.transform;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;

public class TransformBuilder {

  public static Transform build(TransformType type, ParquetMetaInfo metaInfo) {
    Transform transform;
    switch (type) {
      case Value2Null:
        transform = new Value2Null(metaInfo);
        break;
      default:
        throw new UnsupportedOperationException(type + " is not support.");
    }
    return transform;
  }
}
