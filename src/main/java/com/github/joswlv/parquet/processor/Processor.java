package com.github.joswlv.parquet.processor;

import com.github.joswlv.parquet.io.GracefulReader;
import com.github.joswlv.parquet.io.GracefulWriter;
import com.github.joswlv.parquet.io.IoBuilder;
import com.github.joswlv.parquet.io.IoType;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.transform.TransformBuilder;
import com.github.joswlv.parquet.transform.TransformType;
import com.github.joswlv.parquet.transform.Value2Null;
import java.io.IOException;

public class Processor {

  private ParquetMetaInfo metaInfo;
  private String previousFilePath;

  public Processor(ParquetMetaInfo metaInfo, String previousFilePath) {
    this.metaInfo = metaInfo;
    this.previousFilePath = previousFilePath;
  }

  public void process() throws IOException {

    GracefulReader gracefulReader = (GracefulReader) IoBuilder
        .build(IoType.GracefulReader, metaInfo, previousFilePath);
    Value2Null value2Null = (Value2Null) TransformBuilder.build(TransformType.Value2Null, metaInfo);
    GracefulWriter gracefulWriter = (GracefulWriter) IoBuilder
        .build(IoType.GracefulWriter, metaInfo, metaInfo.getOriginSourcePath());

    gracefulReader
        .getData()
        .map(value2Null::transform)
        .forEach(gracefulWriter::write);
  }

}
