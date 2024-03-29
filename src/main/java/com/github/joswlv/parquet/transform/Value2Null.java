package com.github.joswlv.parquet.transform;


import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class Value2Null implements Transform<Group, Group> {

  private String keyColName;
  private List<String> keyColValueList;
  private List<String> targetColNameList;
  private SimpleGroupFactory rowGroupFactory;
  private MessageType schema;

  public Value2Null(ParquetMetaInfo metaInfo) {
    this.keyColName = metaInfo.getKeyColName();
    this.keyColValueList = metaInfo.getKeyColValueList();
    this.targetColNameList = metaInfo.getTargetColNameList();
    this.schema = metaInfo.getSchema();
    this.rowGroupFactory = new SimpleGroupFactory(metaInfo.getSchema());
  }

  @Override
  public Group transform(Group preRecord) {

    if (isNotConvertRow(preRecord)) {
      return preRecord;
    }

    Group newRecord = rowGroupFactory.newGroup();
    int fieldCount = schema.getFieldCount();
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      if (isConvertNullColumn(fieldIndex)) {
        continue;
      }
      addColValue(newRecord, fieldIndex, preRecord);
    }

    return newRecord;
  }

  private boolean isConvertRow(Group group) {
    int keyColIndex = schema.getFieldIndex(keyColName);
    return keyColValueList.contains(group.getValueToString(keyColIndex, 0));
  }

  private boolean isNotConvertRow(Group group) {
    return !isConvertRow(group);
  }

  private boolean isConvertNullColumn(int fieldIndex) {
    return targetColNameList.contains(schema.getFieldName(fieldIndex));
  }

  private boolean isNotConvertNullColumn(int fieldIndex) {
    return !isConvertNullColumn(fieldIndex);
  }

  private void addColValue(Group newRecord, int index, Group preRecord) {
    GroupType type = preRecord.getType();
    PrimitiveTypeName columnType = type.getType(index).asPrimitiveType().getPrimitiveTypeName();
    String fieldName = type.getFieldName(index);

    int fieldRepetitionCount = preRecord.getFieldRepetitionCount(index);
    for (int fieldIndex = 0; fieldIndex < fieldRepetitionCount; fieldIndex++) {
      switch (columnType) {
        case BINARY:
          newRecord
              .add(fieldName, preRecord.getBinary(schema.getFieldName(index), fieldIndex));
          break;
        case BOOLEAN:
          newRecord
              .add(fieldName, preRecord.getBoolean(schema.getFieldName(index), fieldIndex));
          break;
        case DOUBLE:
          newRecord
              .add(fieldName, preRecord.getDouble(schema.getFieldName(index), fieldIndex));
          break;
        case FLOAT:
          newRecord.add(fieldName, preRecord.getFloat(schema.getFieldName(index), fieldIndex));
          break;
        case INT32:
          newRecord
              .add(fieldName, preRecord.getInteger(schema.getFieldName(index), fieldIndex));
          break;
        case INT64:
          newRecord.add(fieldName, preRecord.getLong(schema.getFieldName(index), fieldIndex));
          break;
        case INT96:
          newRecord.add(fieldName, preRecord.getInt96(schema.getFieldName(index), fieldIndex));
          break;
        default:
          throw new IllegalArgumentException("Not Support Type!");
      }
    }
  }

}