package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

public class UdfRegistry {
  public static final String UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE =
      "This function serves as UDF validation and not implemented yet.";
  public static final UdfInfo PRESTO_FROM_ISO8601_TIMESTAMP =
      new UdfInfo(
          "from_iso8601_timestamp",
          ScalarFunctionImpl.create(PrestoUDF.class, "fromIso8601Timestamp"));
  public static final UdfInfo PRESTO_SUBSTR =
      new UdfInfo("substr", ScalarFunctionImpl.create(PrestoUDF.class, "substr"));
  public static final UdfInfo PRESTO_REPLACE =
      new UdfInfo("replace", ScalarFunctionImpl.create(PrestoUDF.class, "replace"));
  public static final UdfInfo PRESTO_DATE_PARSE =
      new UdfInfo("DATE_PARSE", ScalarFunctionImpl.create(PrestoUDF.class, "dateParse"));
  public static final UdfInfo PRESTO_DATE_FORMAT =
      new UdfInfo("date_format", ScalarFunctionImpl.create(PrestoUDF.class, "dateFormat"));
  public static final UdfInfo PRESTO_TO_TIMESTAMP_TZ =
      new UdfInfo("to_timestamp_tz", ScalarFunctionImpl.create(PrestoUDF.class, "toTimestampTz"));
  public static final UdfInfo PRESTO_DATE_DIFF =
      new UdfInfo("DATE_DIFF", ScalarFunctionImpl.create(PrestoUDF.class, "dateDiff"));
  public static final UdfInfo PRESTO_DATE_DIFF_STRING =
      new UdfInfo("DATE_DIFF", ScalarFunctionImpl.create(PrestoUDF.class, "dateDiffString"));
  public static final UdfInfo PRESTO_GREAT_CIRCLE_DISTANCE =
      new UdfInfo(
          "great_circle_distance",
          ScalarFunctionImpl.create(PrestoUDF.class, "greatCircleDistance"));
  public static final UdfInfo PRESTO_AT_TIMEZONE =
      new UdfInfo("at_timezone", ScalarFunctionImpl.create(PrestoUDF.class, "atTimezone"));
  public static final UdfInfo PRESTO_P50 =
      new UdfInfo(
          "approx_percentile",
          AggregateFunctionImpl.create(PrestoUDF.ApproxPercentileAggregate.class));
  public static final UdfInfo HIVE_P50 =
      new UdfInfo(
          "percentile_approx",
          AggregateFunctionImpl.create(PrestoUDF.ApproxPercentileAggregate.class));
  public static final UdfInfo PRESTO_CONCAT =
      new UdfInfo("CONCAT", ScalarFunctionImpl.create(PrestoUDF.class, "concat"));
  public static final UdfInfo HIVE_CONCAT3 =
      new UdfInfo("CONCAT3", ScalarFunctionImpl.create(HiveUDF.class, "concat3"));
  public static final UdfInfo HIVE_CONCAT5 =
      new UdfInfo("CONCAT5", ScalarFunctionImpl.create(HiveUDF.class, "concat5"));
  public static final UdfInfo PRESTO_CONTAINS =
      new UdfInfo("contains", ScalarFunctionImpl.create(PrestoUDF.class, "contains"));
  public static final UdfInfo PRESTO_NOW =
      new UdfInfo("now", ScalarFunctionImpl.create(PrestoUDF.class, "now"));
  public static final UdfInfo PRESTO_CURRENT_TIMESTAMP =
      new UdfInfo("current_timestamp", ScalarFunctionImpl.create(PrestoUDF.class, "now"));
  public static final UdfInfo PRESTO_TO_UNIXTIME =
      new UdfInfo("to_unixtime", ScalarFunctionImpl.create(PrestoUDF.class, "toUnixTime"));
  public static final UdfInfo PRESTO_FROM_UNIXTIME =
      new UdfInfo("from_unixtime", ScalarFunctionImpl.create(PrestoUDF.class, "fromUnixTime"));
  public static final UdfInfo HIVE_FROM_UNIXTIME =
      new UdfInfo("from_unixtime_hive", ScalarFunctionImpl.create(HiveUDF.class, "fromUnixTime"));
  public static final UdfInfo PRESTO_COUNT_DISTINCT =
      new UdfInfo(
          "count_distinct", AggregateFunctionImpl.create(PrestoUDF.CountDistinctAggregate.class));
  public static final UdfInfo PRESTO_MIN_BY =
      new UdfInfo("MIN_BY", AggregateFunctionImpl.create(PrestoUDF.MinByAggregate.class));

  // ================== Hive UDFs ======================
  public static final UdfInfo HIVE_CURRENT_TIMESTAMP =
      new UdfInfo("current_timestamp", ScalarFunctionImpl.create(HiveUDF.class, "currentTimestamp"));
  public static final UdfInfo HIVE_UNIX_TIMESTAMP =
      new UdfInfo("unix_timestamp", ScalarFunctionImpl.create(HiveUDF.class, "unixTimestamp"));
  public static final UdfInfo HIVE_UNIX_TIMESTAMP_FROM_TIMESTAMP =
      new UdfInfo(
          "unix_timestamp_from_timestamp",
          ScalarFunctionImpl.create(HiveUDF.class, "unixTimestampFromTimestamp"));
  public static final UdfInfo HIVE_UNIX_TIMESTAMP_FROM_DATE =
      new UdfInfo(
          "unix_timestamp_from_date",
          ScalarFunctionImpl.create(HiveUDF.class, "unixTimestampFromDate"));
  public static final UdfInfo HIVE_DATE_ADD =
      new UdfInfo("date_add", ScalarFunctionImpl.create(HiveUDF.class, "dateAdd"));
  public static final UdfInfo HIVE_DATE_SUB =
      new UdfInfo("date_sub", ScalarFunctionImpl.create(HiveUDF.class, "dateSub"));
  public static final UdfInfo HIVE_DATE_TRUNC =
      new UdfInfo("date_trunc", ScalarFunctionImpl.create(HiveUDF.class, "dateTrunc"));
  public static final UdfInfo HIVE_GET_HEXAGON_ADDR =
      new UdfInfo("get_hexagon_addr", ScalarFunctionImpl.create(HiveUDF.class, "getHexagonAddr"));
  public static final UdfInfo HIVE_ST_CONTAINS =
      new UdfInfo("ST_CONTAINS", ScalarFunctionImpl.create(HiveUDF.class, "stContains"));
  public static final UdfInfo HIVE_ST_GEOM_FROM_TEXT =
      new UdfInfo("ST_GeomFromText", ScalarFunctionImpl.create(HiveUDF.class, "stGeomFromText"));
  public static final UdfInfo HIVE_ST_POINT =
      new UdfInfo("ST_POINT", ScalarFunctionImpl.create(HiveUDF.class, "stPoint"));
  public static final UdfInfo HIVE_DATE_FORMAT =
      new UdfInfo("date_format", ScalarFunctionImpl.create(HiveUDF.class, "hiveDateFormat"));
  public static final UdfInfo HIVE_FROM_UTC_TIMESTAMP =
      new UdfInfo(
          "from_utc_timestamp", ScalarFunctionImpl.create(HiveUDF.class, "fromUtcTimestamp"));
}
