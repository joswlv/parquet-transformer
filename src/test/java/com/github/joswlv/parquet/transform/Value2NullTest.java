package com.github.joswlv.parquet.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.joswlv.parquet.TestHdfsBuilder;
import com.github.joswlv.parquet.io.GracefulReader;
import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Value2NullTest extends TestHdfsBuilder {

  private ParquetMetaInfo parquetMetaInfo;
  private List<GracefulReader> gracefulReaderList;
  private Value2Null value2Null;
  private String requiredArg;

  @BeforeEach
  void Before() throws IOException {
    requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=1;100 --target-col-name=email;gender --concurrent=3 --default-fs=hdfs://localhost:11000";
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
    value2Null = new Value2Null(parquetMetaInfo);

    gracefulReaderList = new ArrayList<>();
    for (String parquetFile : parquetMetaInfo.getPreviousFileList()) {
      gracefulReaderList.add(new GracefulReader(parquetMetaInfo, parquetFile));
    }
  }

  /**
   * null로 변경되어야하는 Row(=Group)의 수가 일치하는가
   */
  @Test
  void checkConvertedGroupCount() throws IOException {

    int convertedValueCount = parquetMetaInfo.getKeyColValueList().size();

    int convertedValue = 0;
    for (GracefulReader gracefulReader : gracefulReaderList) {
      convertedValue += gracefulReader.getData()
          .filter(group -> !group.equals(value2Null.transform(group))).count();
    }

    assertEquals(convertedValueCount, convertedValue);

  }

  /**
   * null로 변경되어도, Row(=Group)이 가진 필드의 개수는 일정한가? null로 변경된 Row를 print하면 마치 필드가 사라진것 처럼 보이나, 실제는 null로
   * 처리되어 있음
   */
  @Test
  void checkConvertedGroupSameFieldCount() {

    for (GracefulReader gracefulReader : gracefulReaderList) {
      gracefulReader.getData().forEach(group -> {
        assertEquals(group.getType().getFieldCount(),
            value2Null.transform(group).getType().getFieldCount());
      });
    }

  }


  /**
   * null로 변경되기전의 Row의 RepetitionCount는 그대로 유지되었는가
   */
  @Test
  void checkConvertedFieldisNotNull() {

    List<String> targetColNameList = parquetMetaInfo.getTargetColNameList();

    for (GracefulReader gracefulReader : gracefulReaderList) {
      List<Group> convertedGroupList = gracefulReader
          .getData()
          .filter(group -> !group.equals(value2Null.transform(group)))
          .collect(Collectors.toList());

      for (Group group : convertedGroupList) {
        for (String targetColName : targetColNameList) {
          assertEquals(group.getFieldRepetitionCount(targetColName), 1);
        }
      }
    }

  }


  /**
   * null로 변경되었는가? null로 변경시, 해당 필드의 RepetitionCount가 0으로 변경
   */
  @Test
  void checkConvertedFieldisNull() {

    List<String> targetColNameList = parquetMetaInfo.getTargetColNameList();

    for (GracefulReader gracefulReader : gracefulReaderList) {

      List<Group> convertedGroupList = gracefulReader
          .getData()
          .filter(group -> !group.equals(value2Null.transform(group)))
          .map(group -> value2Null.transform(group))
          .collect(Collectors.toList());

      for (Group group : convertedGroupList) {
        for (String targetColName : targetColNameList) {
          assertEquals(group.getFieldRepetitionCount(targetColName), 0);
        }
      }
    }

  }

}