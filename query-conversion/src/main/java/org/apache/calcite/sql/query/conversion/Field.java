package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;
import org.apache.calcite.sql.query.conversion.Schema.FieldType;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;

import static java.lang.String.format;

/** Class representing field in datasource logical schema. */
@lombok.EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Field {

  private final String name;
  private final FieldType type;
  private final boolean dimension;
  private final boolean metric;
  private final Boolean optional;
  private final List<Field> fields;
  private final Field itemField;
  private final Integer cardinality;
  private final boolean index;
  private final GeoShape geoShape;
  private final Boolean esDisableFieldData;

  /**
   * @param name name of field.
   * @param type type of field.
   * @param dimension is dimension (can be grouped by).
   * @param metric is metric (can be aggregated).
   * @param optional is optional.
   * @param fields list of sub-fields if this field has object type.
   * @param itemField array item field, if this field has array type
   * @param cardinality cardinality of field.
   * @param index should this be indexed or not.
   * @param geoShape indexing options for geoShape geoShape type.
   * @param esDisableFieldData disable field data (only for ES storages).
   */
  @JsonCreator
  public Field(
      @JsonProperty(value = "name", required = true) @NotNull String name,
      @JsonProperty(value = "type", required = true) @NotNull FieldType type,
      @JsonProperty(value = "dimension") @DefaultValue("false") Boolean dimension,
      @JsonProperty(value = "metric") @DefaultValue("false") Boolean metric,
      @JsonProperty(value = "optional") @DefaultValue("true") Boolean optional,
      @JsonProperty(value = "fields") List<Field> fields,
      @JsonProperty(value = "itemField") Field itemField,
      @JsonProperty(value = "cardinality") Integer cardinality,
      @JsonProperty(value = "index") @DefaultValue("true") Boolean index,
      @JsonProperty(value = "geoShape") GeoShape geoShape,
      @JsonProperty(value = "esDisableFieldData") Boolean esDisableFieldData) {

    this.name = name;
    this.type = type;
    this.dimension = dimension != null ? dimension : false;
    this.metric = metric != null ? metric : false;
    this.optional = optional != null ? optional : true;
    this.fields = fields;
    this.itemField = itemField;
    this.index = index != null ? index : true;
    this.geoShape = geoShape;
    this.esDisableFieldData = esDisableFieldData;

    // Check that if this field is not object
    final boolean notMetricOrDimension = !this.metric && !this.dimension;
    if (!this.type.hasComplexObjectFields() && notMetricOrDimension && this.index) {
      throw new IllegalArgumentException(
          format("Field %s must be metric or dimension or both", name));
    }

    // Check that dimension has cardinality, except it's boolean (trivial) or date.
    if (this.type == FieldType.BOOLEAN) {
      this.cardinality = 2;
    } else {
      this.cardinality = cardinality;
    }

    // Check array type.
    if (this.type == FieldType.ARRAY && this.itemField == null) {
      throw new IllegalArgumentException(
          format("Array field [%s] must have itemField specified.", name));
    } else if (this.type != FieldType.ARRAY && this.itemField != null) {
      throw new IllegalArgumentException(
          format("itemField [%s] must be null for non-array type.", name));
    }

    // Check object type.
    if (this.type.hasComplexObjectFields()
        && (this.fields == null || this.fields.size() == 0)
        && !Boolean.FALSE.equals(index)) {
      throw new IllegalArgumentException(
          format(
              "Object or nested field [%s] must have fields "
                  + "specified and contain at least one field.",
              name));
    } else if (!this.type.hasComplexObjectFields() && this.fields != null) {
      throw new IllegalArgumentException(
          format("Fields [%s] must be null for non-object or non-nested type.", name));
    }

    // Check that geo_shape type has geoShape field.
    if (this.type == FieldType.GEO_SHAPE && this.geoShape == null) {
      throw new IllegalArgumentException(
          format("Geo shape field [%s] must specify options in `geoShape` field.", name));
    }
  }

  @Override
  public String toString() {
    final String fieldParam;
    if (metric && dimension) {
      fieldParam = "";
    } else if (metric) {
      fieldParam = ", metric";
    } else if (dimension) {
      fieldParam = ", dimension";
    } else {
      fieldParam = "";
    }
    final String typeParam;
    if (FieldType.ARRAY.equals(type)) {
      typeParam = "[" + itemField.type().toString().toLowerCase() + "]";
    } else {
      typeParam = "";
    }
    return format("Field(%s, %s%s%s)", name, type.toString().toLowerCase(), typeParam, fieldParam);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(@NonNull Field copy) {
    return new Builder(copy);
  }

  @JsonProperty("name")
  public String name() {
    return name;
  }

  @JsonProperty("type")
  public FieldType type() {
    return type;
  }

  @JsonProperty("dimension")
  public boolean dimension() {
    return dimension;
  }

  @JsonProperty("metric")
  public boolean metric() {
    return metric;
  }

  @JsonProperty("optional")
  public Boolean optional() {
    return optional;
  }

  @JsonProperty("fields")
  public List<Field> fields() {
    return fields;
  }

  @JsonProperty("itemField")
  public Field itemField() {
    return itemField;
  }

  @JsonProperty("cardinality")
  public Integer cardinality() {
    return cardinality;
  }

  @JsonProperty("index")
  public boolean index() {
    return index;
  }

  @JsonProperty("geoShape")
  public GeoShape shape() {
    return geoShape;
  }

  @JsonProperty("esDisableFieldData")
  public Boolean esDisableFieldData() {
    return esDisableFieldData;
  }

  public static final class Builder {

    private String name;
    private FieldType type;
    private Boolean dimension;
    private Boolean metric;
    private Boolean optional;
    private List<Field> fields;
    private Field itemField;
    private Integer cardinality;
    private Boolean index;
    private GeoShape geoShape;
    private Boolean esDisableFieldData;

    public Builder() {}

    public Builder(@NonNull Field copy) {
      this.name = copy.name;
      this.type = copy.type;
      this.dimension = copy.dimension;
      this.metric = copy.metric;
      this.optional = copy.optional;
      this.fields = copy.fields;
      this.itemField = copy.itemField;
      this.cardinality = copy.cardinality;
      this.index = copy.index;
      this.geoShape = copy.geoShape;
      this.esDisableFieldData = copy.esDisableFieldData;
    }

    public Field build() {
      return new Field(
          name,
          type,
          dimension,
          metric,
          optional,
          fields,
          itemField,
          cardinality,
          index,
          geoShape,
          esDisableFieldData);
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(FieldType type) {
      this.type = type;
      return this;
    }

    public Builder dimension(Boolean dimension) {
      this.dimension = dimension;
      return this;
    }

    public Builder metric(Boolean metric) {
      this.metric = metric;
      return this;
    }

    public Builder optional(Boolean optional) {
      this.optional = optional;
      return this;
    }

    public Builder fields(List<Field> fields) {
      this.fields = fields;
      return this;
    }

    public Builder itemField(Field itemField) {
      this.itemField = itemField;
      return this;
    }

    public Builder cardinality(Integer cardinality) {
      this.cardinality = cardinality;
      return this;
    }

    public Builder index(Boolean index) {
      this.index = index;
      return this;
    }

    public Builder geoShape(GeoShape geoShape) {
      this.geoShape = geoShape;
      return this;
    }

    public Builder esDisableFieldData(Boolean esDisableFieldData) {
      this.esDisableFieldData = esDisableFieldData;
      return this;
    }
  }
}