package org.apache.calcite.sql.query.conversion.udf;

import java.sql.Date;
import java.sql.Timestamp;

import static org.apache.calcite.sql.query.conversion.udf.UdfRegistry.UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE;

/**
 * HiveUDF holds Hive UDFs.
 *
 * <p>Semantics and signatures of these functions could be found at
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
 *
 * <p>Since we only need these UDFs for validation purpose and delegate execution to Hive itself, we
 * don't fill in these UDFs bodies.
 */
public class HiveUDF {
  public Timestamp current_timestamp() {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long unixTimestamp(String timetamp, String format) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long unixTimestampFromTimestamp(Timestamp timetamp) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long unixTimestampFromDate(Date date) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Date dateAdd(Date date, Integer days) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Date dateSub(Date date, Integer days) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Date dateTrunc(String type, Timestamp timestamp) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String getHexagonAddr(double lat, double lon, int level) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String concat3(String s1, String s2, String s3) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String concat5(String s1, String s2, String s3, String s4, String s5) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String fromUnixTime(long timestamp, String format) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String stGeomFromText(String shape) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String stPoint(double lat, double lng) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String stContains(String geom, String point) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }
}
