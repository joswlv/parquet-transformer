package com.github.joswlv.parquet.util;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ParquetMetaInfoChecker {

  private ParquetMetaInfo parquetMetaInfo;

  public ParquetMetaInfoChecker(ParquetMetaInfo parquetMetaInfo) {
    this.parquetMetaInfo = parquetMetaInfo;
  }

  public boolean isAddParquetFile(String parquetFileString) throws IOException {

    Path parquetFile = new Path(parquetFileString);

    if (isAddAllParquetFile(parquetMetaInfo)) {
      return true;
    }

    ParquetFileReader fileReader = ParquetFileReader.open(parquetMetaInfo.getConfiguration(),
        parquetFile);

    int keyColIndex = fileReader.getFileMetaData().getSchema()
        .getFieldIndex(parquetMetaInfo.getKeyColName());
    List<String> keyColValueList = parquetMetaInfo.getKeyColValueList();

    List<BlockMetaData> rowGroupsList = fileReader.getRowGroups();
    for (BlockMetaData blockMetaData : rowGroupsList) {

      if (isNotExistStatisticInfo(blockMetaData, keyColIndex)) {
        return true;
      }

      if (isExistColValueInRowGroup(blockMetaData, keyColIndex, keyColValueList)) {
        return true;
      }
    }
    return false;
  }

  private boolean isAddAllParquetFile(ParquetMetaInfo parquetMetaInfo) {
    return !parquetMetaInfo.isSkipFileOption();
  }

  private boolean isExistStatisticInfo(BlockMetaData blockMetaData, int keyColIndex) {
    return blockMetaData.getColumns().get(keyColIndex).getStatistics().genericGetMax() != null;
  }

  private boolean isNotExistStatisticInfo(BlockMetaData blockMetaData, int keyColIndex) {
    return !isExistStatisticInfo(blockMetaData, keyColIndex);
  }

  private boolean isExistColValueInRowGroup(BlockMetaData blockMetaData, int keyColIndex,
      List<String> keyColValueList) {

    ColumnChunkMetaData columnChunkMetaData = blockMetaData.getColumns().get(keyColIndex);

    for (String keyColValue : keyColValueList) {

      int compareMinResult = compareStatistics(columnChunkMetaData, keyColValue,
          new MinStatisticsCompare());
      int compareMaxResult = compareStatistics(columnChunkMetaData, keyColValue,
          new MaxStatisticsCompare());

      if (compareMinResult <= 0 && compareMaxResult >= 0) {
        return true;
      }
    }

    return false;

  }

  private int compareStatistics(ColumnChunkMetaData columnChunkMetaData, String keyColValue,
      StatisticsCompare statisticsCompare) {

    PrimitiveTypeName primitiveTypeName = columnChunkMetaData.getPrimitiveType()
        .getPrimitiveTypeName();

    Statistics statistics = columnChunkMetaData.getStatistics();
    switch (primitiveTypeName) {
      case BINARY:
      case INT96:
        return statisticsCompare.compare(statistics, Binary.fromString(keyColValue));
      case DOUBLE:
        return statisticsCompare.compare(statistics, Double.parseDouble(keyColValue));
      case FLOAT:
        return statisticsCompare.compare(statistics, Float.parseFloat(keyColValue));
      case INT32:
        return statisticsCompare.compare(statistics, Integer.parseInt(keyColValue));
      case INT64:
        return statisticsCompare.compare(statistics, Long.parseLong(keyColValue));
      default:
        throw new IllegalArgumentException("Not Support Type!");
    }
  }
}



