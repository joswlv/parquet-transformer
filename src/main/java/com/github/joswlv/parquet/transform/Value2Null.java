package com.github.joswlv.parquet.transform;


import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class Value2Null implements Transform<Group, Group> {

  private List<String> targetColNameList;
  private SimpleGroupFactory rowGroupFactory;
  private MessageType schema;

  public Value2Null(ParquetMetaInfo metaInfo) {
    this.targetColNameList = metaInfo.getTargetColNameList();
    this.schema = metaInfo.getSchema();
    this.rowGroupFactory = new SimpleGroupFactory(metaInfo.getSchema());
  }

  @Override
  public Group transform(Group preRecord) {
    Group newRecord = rowGroupFactory.newGroup();
    int fieldCount = schema.getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      if (targetColNameList.contains(schema.getFieldName(i))) {
        //If you pass an index, the index value is filled with null.
        continue;
      }
      addColValue(newRecord, i, preRecord);
    }

    return newRecord;
  }

  private void addColValue(Group newRecord, int index, Group preRecord) {
    GroupType type = preRecord.getType();
    PrimitiveTypeName columnType = type.getType(index).asPrimitiveType()
        .getPrimitiveTypeName();
    String fieldName = type.getFieldName(index);

    switch (columnType) {
      case BINARY:
        newRecord.add(fieldName, preRecord.getBinary(schema.getFieldName(index), index));
        break;
      case BOOLEAN:
        newRecord.add(fieldName, preRecord.getBoolean(schema.getFieldName(index), index));
        break;
      case DOUBLE:
        newRecord.add(fieldName, preRecord.getDouble(schema.getFieldName(index), index));
        break;
      case FLOAT:
        newRecord.add(fieldName, preRecord.getFloat(schema.getFieldName(index), index));
        break;
      case INT32:
        newRecord.add(fieldName, preRecord.getInteger(schema.getFieldName(index), index));
        break;
      case INT64:
        newRecord.add(fieldName, preRecord.getLong(schema.getFieldName(index), index));
        break;
      case INT96:
        newRecord.add(fieldName, preRecord.getInt96(schema.getFieldName(index), index));
        break;
      default:
        throw new IllegalArgumentException("Not Support Type!");
    }
  }

}
