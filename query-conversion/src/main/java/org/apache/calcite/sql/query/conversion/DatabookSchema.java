package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;

@Getter
public class DatabookSchema {
  private final String tableName;
  private final String database;
  private final String storageType;
  private final List<Column> columns;

  @JsonCreator
  public DatabookSchema(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("database") String database,
      @JsonProperty("storageType") String storageType,
      @JsonProperty("columns") List<Column> columns) {
    this.tableName = tableName;
    this.database = database;
    this.storageType = storageType;
    this.columns = columns;
  }

  public RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory) {
    List<String> fieldNameList = new ArrayList<>();
    List<RelDataType> relDataTypeList = new ArrayList<>();
    for (Column column : columns) {
      fieldNameList.add(column.getName());
      relDataTypeList.add(column.getNestedDataTypes().convertToRelDataType(relDataTypeFactory));
    }
    return relDataTypeFactory.createStructType(
        StructKind.PEEK_FIELDS_DEFAULT, relDataTypeList, fieldNameList);
  }

  @Getter
  public static class Column {
    private final String name;
    private final AttributeDataType nestedDataTypes;

    @JsonCreator
    public Column(
        @JsonProperty("name") String name,
        @JsonProperty("nestedDataTypes") AttributeDataType nestedDataTypes) {
      this.name = name;
      this.nestedDataTypes = nestedDataTypes;
    }
  }

  @Getter
  public static class Attribute {
    private final String attributeName;
    private final AttributeDataType attributeDataType;

    public Attribute(
        @JsonProperty("attributeName") String attributeName,
        @JsonProperty("attributeDataType") AttributeDataType attributeDataType) {
      this.attributeName = attributeName;
      this.attributeDataType = attributeDataType;
    }
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type",
      include = JsonTypeInfo.As.PROPERTY,
      defaultImpl = SimpleAttributeDataType.class,
      visible = true
  )
  @JsonSubTypes({
      @JsonSubTypes.Type(value = StructAttributeDataType.class, name = "STRUCT"),
      @JsonSubTypes.Type(value = ArrayAttributeDataType.class, name = "ARRAY"),
      @JsonSubTypes.Type(value = MapAttributeDataType.class, name = "MAP")
  })
  public interface AttributeDataType {
    String JSON_KEY_TYPE = "type";

    RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory);

    Schema.FieldType getType();
  }

  @Getter
  public static class SimpleAttributeDataType implements AttributeDataType {
    private final Schema.FieldType type;

    @JsonCreator
    public SimpleAttributeDataType(@JsonProperty(JSON_KEY_TYPE) String type) {
      if (StringUtils.equalsIgnoreCase(type, "timestamp")
          || StringUtils.equalsIgnoreCase(type, "date")) {
        this.type = Schema.FieldType.DATETIME;
      } else if (StringUtils.equalsIgnoreCase(type, "bigint")) {
        this.type = Schema.FieldType.LONG;
      } else if (StringUtils.equalsIgnoreCase(type, "int")) {
        this.type = Schema.FieldType.INTEGER;
      } else if (StringUtils.equalsIgnoreCase(type, "BINARY")) {
        this.type = Schema.FieldType.STRING;
      } else {
        this.type = Schema.FieldType.valueOf(type);
      }
    }

    @Override
    public RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory) {
      switch (this.getType()) {
        case TEXT:
        case STRING:
          return relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR);
        case FLOAT:
        case DOUBLE:
          return relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE);
        case SHORT:
        case INTEGER:
          return relDataTypeFactory.createSqlType(SqlTypeName.INTEGER);
        case LONG:
          return relDataTypeFactory.createSqlType(SqlTypeName.BIGINT);
        case BOOLEAN:
          return relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
        case DATETIME:
          return relDataTypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        default:
          throw new IllegalArgumentException(this.getType() + " not supported");
      }
    }
  }

  @Getter
  public static class ArrayAttributeDataType implements AttributeDataType {
    private final AttributeDataType elementType;

    @JsonCreator
    public ArrayAttributeDataType(@JsonProperty("elementType") AttributeDataType elementType) {
      this.elementType = elementType;
    }

    @Override
    public RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory) {
      return relDataTypeFactory.createArrayType(
          elementType.convertToRelDataType(relDataTypeFactory), -1);
    }

    public Schema.FieldType getType() {
      return Schema.FieldType.ARRAY;
    }
  }

  @Getter
  public static class MapAttributeDataType implements AttributeDataType {
    private final SimpleAttributeDataType keyType;
    private final AttributeDataType valueType;

    @JsonCreator
    public MapAttributeDataType(
        @JsonProperty("keyType") SimpleAttributeDataType keyType,
        @JsonProperty("valueType") AttributeDataType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory) {
      return relDataTypeFactory.createMapType(
          keyType.convertToRelDataType(relDataTypeFactory),
          valueType.convertToRelDataType(relDataTypeFactory));
    }

    /**
     * Temporarily use Gairos OBJECT as map and Gairos NESTED as struct because there is no MAP in
     * Gairos FieldType.
     */
    @Override
    public Schema.FieldType getType() {
      return Schema.FieldType.OBJECT;
    }
  }

  @Getter
  public static class StructAttributeDataType implements AttributeDataType {
    private final List<Attribute> attributes;

    @JsonCreator
    public StructAttributeDataType(@JsonProperty("attributes") List<Attribute> attributes) {
      this.attributes = attributes;
    }

    @Override
    public RelDataType convertToRelDataType(RelDataTypeFactory relDataTypeFactory) {
      List<String> fieldNameList = new ArrayList<>();
      List<RelDataType> relDataTypeList = new ArrayList<>();
      for (Attribute attribute : attributes) {
        fieldNameList.add(attribute.getAttributeName());
        relDataTypeList.add(
            attribute.getAttributeDataType().convertToRelDataType(relDataTypeFactory));
      }
      return relDataTypeFactory.createStructType(
          StructKind.PEEK_FIELDS_DEFAULT, relDataTypeList, fieldNameList);
    }

    public Schema.FieldType getType() {
      return Schema.FieldType.NESTED;
    }
  }
}

