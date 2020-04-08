package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.Size;

import static java.lang.String.join;

/**
 * Datasource logical schema.
 *
 * @author zaytsev
 */
@lombok.EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Schema {

  private static final String PRINT_INDENT = "  ";

  /** Serializable enum for Datasource schema field types. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  public enum FieldType {
    OBJECT(true),
    ARRAY,
    TEXT,
    STRING,
    FLOAT,
    DOUBLE,
    SHORT,
    INTEGER,
    LONG,
    BOOLEAN,
    DATETIME,
    GEO_SHAPE,
    NESTED(true);

    public static final Map<String, FieldType> DE_MAP =
        new ImmutableMap.Builder<String, FieldType>()
            .put(OBJECT.name().toLowerCase(), OBJECT)
            .put(ARRAY.name().toLowerCase(), ARRAY)
            .put(TEXT.name().toLowerCase(), TEXT)
            .put(STRING.name().toLowerCase(), STRING)
            .put(FLOAT.name().toLowerCase(), FLOAT)
            .put(DOUBLE.name().toLowerCase(), DOUBLE)
            .put(SHORT.name().toLowerCase(), SHORT)
            .put(INTEGER.name().toLowerCase(), INTEGER)
            .put(LONG.name().toLowerCase(), LONG)
            .put(BOOLEAN.name().toLowerCase(), BOOLEAN)
            .put(DATETIME.name().toLowerCase(), DATETIME)
            .put(GEO_SHAPE.name().toLowerCase(), GEO_SHAPE)
            .put(NESTED.name().toLowerCase(), NESTED)
            .build();

    private boolean hasComplexObjectFields;

    FieldType(boolean hasComplexObjectFields) {
      this.hasComplexObjectFields = hasComplexObjectFields;
    }

    FieldType() {
      this.hasComplexObjectFields = false;
    }

    public boolean hasComplexObjectFields() {
      return this.hasComplexObjectFields;
    }

    @JsonCreator
    public static FieldType forValue(String value) {
      final FieldType fieldType = DE_MAP.get(value);
      if (fieldType != null) {
        return fieldType;
      }
      throw new IllegalArgumentException("Unrecognized field type " + value);
    }

    @JsonValue
    public String toValue() {
      return name().toLowerCase();
    }
  }

  private final String timeField;
  private final List<RequiredFilter> requiredFilters;
  private final List<Field> fields;
  private final List<Alias> aliases;
  private final List<String> esExcludes;

  private final Map<String, Field> flatFields;
  private final Map<String, Alias> flatAliases;
  private final Map<String, Field> flatDimensions;
  private final Map<String, Field> flatMetrics;

  /**
   * @param timeField default time dimension field.
   * @param requiredFilters list of fields and their required filters for grouping by.
   * @param fields top level fields of logical schema.
   * @param aliases list of fields and their alias fields.
   * @param esExcludes list of fields to exclude from source (only for ES storages).
   */
  @JsonCreator
  public Schema(
      @JsonProperty(value = "timeField", required = true) String timeField,
      @JsonProperty(value = "requiredFilters") List<RequiredFilter> requiredFilters,
      @JsonProperty(value = "fields", required = true) @Size(min = 1) List<Field> fields,
      @JsonProperty(value = "aliases") List<Alias> aliases,
      @JsonProperty(value = "esExcludes") List<String> esExcludes) {
    this.timeField = timeField;
    this.requiredFilters = requiredFilters;
    this.fields = fields;
    this.aliases = aliases;
    this.esExcludes = esExcludes;
    this.flatFields = buildFlatFields(this);
    this.flatAliases = buildFlatAliases(this);
    this.flatDimensions =
        ImmutableMap.copyOf(
            flatFields
                .entrySet()
                .stream()
                .filter(entry -> Boolean.TRUE.equals(entry.getValue().dimension()))
                .collect(Collectors.toList()));
    this.flatMetrics =
        ImmutableMap.copyOf(
            flatFields
                .entrySet()
                .stream()
                .filter(entry -> Boolean.TRUE.equals(entry.getValue().metric()))
                .collect(Collectors.toList()));
  }

  @JsonProperty("timeField")
  public String timeField() {
    return timeField;
  }

  @JsonProperty("requiredFilters")
  public List<RequiredFilter> requiredFilters() {
    return requiredFilters;
  }

  @JsonProperty("fields")
  public List<Field> fields() {
    return fields;
  }

  @JsonProperty("aliases")
  public List<Alias> aliases() {
    return aliases;
  }

  @JsonProperty("esExcludes")
  public List<String> esExcludes() {
    return esExcludes;
  }

  /** Returns a map of all aliases from alias to alias definition. */
  public Map<String, Alias> flatAliases() {
    return flatAliases;
  }

  /** Returns a map of all fields from field name to field definition. */
  public Map<String, Field> flatFields() {
    return flatFields;
  }

  /** Builds flat fields map using {@link Schema#traverseAndBuildFlatFields} */
  private static Map<String, Field> buildFlatFields(Schema schema) {
    final Map<String, Field> flatFieldMap = new HashMap<>();
    traverseAndBuildFlatFields(schema.fields(), new ArrayList<>(), flatFieldMap);
    return flatFieldMap;
  }

  /**
   * Recursively traverses schema tree and insert field into flat field map every time it reaches
   * leaf node.
   *
   * @param fields fields on current level.
   * @param curPath current path in schema fields tree.
   * @param outFlatFields output flat map.
   */
  private static void traverseAndBuildFlatFields(
      List<Field> fields, List<String> curPath, Map<String, Field> outFlatFields) {
    for (Field field : fields) {
      curPath.add(field.name());
      // If leaf node (not object), add field to output map,
      // Otherwise traverse it.
      if (field.type().hasComplexObjectFields() && field.fields() != null) {
        traverseAndBuildFlatFields(field.fields(), curPath, outFlatFields);
      } else {
        final String flatName = join(".", curPath);
        outFlatFields.put(flatName, field);
      }
      curPath.remove(curPath.size() - 1);
    }
  }

  /** Iterates all aliases in the schema and constructs a map from alias to alias definition. */
  private static Map<String, Alias> buildFlatAliases(Schema schema) {
    final Map<String, Alias> flatAliasMap = new HashMap<>();

    if (schema.aliases() != null) {
      for (Alias alias : schema.aliases()) {
        for (String aliasName : alias.aliases()) {
          flatAliasMap.put(aliasName, alias);
        }
      }
    }

    return flatAliasMap;
  }

  /*
   * Returns a subset of flat fields where metric() == TRUE.
   */
  public Map<String, Field> allMetrics() {
    return flatMetrics;
  }

  /** Returns a subset of flat fields where dimension() == TRUE. */
  public Map<String, Field> allDimensions() {
    return flatDimensions;
  }

  /**
   * Helper method which returns a simple schema containing only fields and not the other class
   * properties.
   *
   * @param timeFieldIndex field which will be used as a time field (e.g. partition key).
   * @param fields list of fields of schema.
   */
  @VisibleForTesting
  public static Schema simpleSchema(int timeFieldIndex, @NonNull List<Field> fields) {
    return new Schema(
        fields.get(timeFieldIndex).name(),
        Collections.emptyList(),
        fields,
        Collections.emptyList(),
        Collections.emptyList());
  }

  /**
   * Helper method which returns a simple schema containing only fields and not the other class
   * properties.
   *
   * @param timeFieldIndex field which will be used as a time field (e.g. partition key).
   * @param fields array of fields of schema.
   */
  @VisibleForTesting
  public static Schema simpleSchema(int timeFieldIndex, @NonNull Field... fields) {
    return simpleSchema(timeFieldIndex, Arrays.asList(fields));
  }

  /**
   * Returns pretty print of the schema.
   *
   * <p>For example:
   *
   * <pre>
   * .
   * ├─ 1 ezpz : OBJECT
   * │  ├─ 1.1 cityId : STRING (dimension)
   * │  ├─ 1.2 fare : FLOAT (metric)
   * │  └─ 1.3 tripUUID : STRING (dimension)
   * ├─ 2 fareUUID : STRING (dimension)
   * ├─ 3 fuf : OBJECT
   * │  ├─ 3.1 dst : OBJECT
   * │  │  ├─ 3.1.1 hexagon_id_9 : STRING (dimension)
   * │  │  ├─ 3.1.2 lat : FLOAT
   * │  │  └─ 3.1.3 lng : FLOAT
   * │  ├─ 3.2 fare : FLOAT (metric)
   * │  ├─ 3.3 group : STRING (dimension)
   * │  ├─ 3.4 ori : OBJECT
   * │  │  ├─ 3.4.1 hexagon_id_9 : STRING (dimension)
   * │  │  ├─ 3.4.2 lat : FLOAT
   * │  │  └─ 3.4.3 lng : FLOAT
   * │  ├─ 3.5 timeLocationIndex : STRING (dimension)
   * │  └─ 3.6 userUUID : STRING (dimension)
   * └─ 4 requestTimestamp : DATETIME (dimension)
   * </pre>
   */
  public String print() {
    return Schema.prettyPrint(fields);
  }

  /** Prints list of fields. */
  public static String prettyPrint(@NonNull List<Field> fields) {
    return prettyPrint(fields, new StringBuilder(".\n"), 0, true, "").toString();
  }

  /** Prints a list of fields of a given level. */
  private static StringBuilder prettyPrint(
      @NonNull List<Field> fields,
      @NonNull StringBuilder sb,
      int level,
      boolean lastField,
      @NonNull String parentNumber) {

    if (level > 32) {
      throw new IllegalStateException("Exceeded maximum expected level.");
    }

    for (int i = 0; i < fields.size(); ++i) {

      final Field field = fields.get(i);

      for (int k = 0; k < level; ++k) {
        if (k == 0 || (k == level - 1 && !lastField)) {
          sb.append("│  ");
        } else {
          sb.append("   ");
        }
      }

      sb.append(i == fields.size() - 1 ? "└─" : "├─");

      final String fieldParam;
      if (field.metric() && field.dimension()) {
        fieldParam = "";
      } else if (field.metric()) {
        fieldParam = " (metric)";
      } else if (field.dimension()) {
        fieldParam = " (dimension)";
      } else {
        fieldParam = "";
      }
      final String typeParam;
      if (FieldType.ARRAY.equals(field.type())) {
        typeParam = "[" + field.itemField().type() + "]";
      } else {
        typeParam = "";
      }

      final String number = parentNumber + (i + 1);

      sb.append(" ")
          .append(number)
          .append(" ")
          .append(field.name())
          .append(" : ")
          .append(field.type())
          .append(typeParam)
          .append(fieldParam)
          .append("\n");

      if (field.type().hasComplexObjectFields()) {
        prettyPrint(field.fields(), sb, level + 1, i == fields.size() - 1, number + ".");
      }
    }
    return sb;
  }

  /** Allows to specify which filters are required if user wants to group by certain fields. */
  @lombok.Value
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Alias {

    private final String field;
    private final List<String> aliases;

    /**
     * @param field name of field.
     * @param aliases list of alternate field names for this field.
     */
    public Alias(
        @JsonProperty(value = "field", required = true) String field,
        @JsonProperty(value = "aliases", required = true) @Size(min = 1) List<String> aliases) {
      this.field = field;
      this.aliases = aliases;
    }

    @JsonProperty("field")
    public String field() {
      return field;
    }

    @JsonProperty("aliases")
    public List<String> aliases() {
      return aliases;
    }
  }

  /** Allows to specify which filters are required if user wants to group by certain fields. */
  @lombok.Value
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class RequiredFilter {

    private final String field;
    private final List<String> filters;

    /**
     * @param field name of field.
     * @param filters list of fields required to have a filter on them.
     */
    public RequiredFilter(
        @JsonProperty(value = "field", required = true) String field,
        @JsonProperty(value = "filters", required = true) @Size(min = 1) List<String> filters) {
      this.field = field;
      this.filters = filters;
    }

    @JsonProperty("field")
    public String field() {
      return field;
    }

    @JsonProperty("filters")
    public List<String> filters() {
      return filters;
    }
  }
}
