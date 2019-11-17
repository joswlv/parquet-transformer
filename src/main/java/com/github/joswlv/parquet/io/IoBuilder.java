package com.github.joswlv.parquet.io;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;

public class IoBuilder {

  public static <T extends IoProcessor> IoProcessor build(IoType type, ParquetMetaInfo metaInfo,
      String filePath) throws IOException {
    IoProcessor ioProcessor;
    switch (type) {
      case GracefulReader:
        ioProcessor = new GracefulReader(metaInfo, filePath);
        break;
      case GracefulWriter:
        ioProcessor = new GracefulWriter(metaInfo, filePath);
        break;
      default:
        throw new UnsupportedOperationException(type + " is not support.");
    }

    return ioProcessor;
  }

}
