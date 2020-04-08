package org.apache.calcite.sql.query.conversion.udf;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.calcite.sql.query.conversion.udf.UdfRegistry.UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE;

/**
 * PrestoUDF holds Presto UDFs.
 *
 * <p>Semantics and signatures of these functions could be found at
 * https://prestodb.github.io/docs/current/functions
 *
 * <p>Since we only need these UDFs for validation purpose and delete execution to Presto itself, we
 * don't fill in these UDFs bodies.
 */
public class PrestoUDF {
  public Timestamp current_timestamp() {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Timestamp now() {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long fromIso8601Timestamp(String timestamp) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Double toUnixTime(Timestamp tt) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Timestamp fromUnixTime(double value) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String concat(String str1, String str2) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String substr(String original, int begin, int end) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String replace(String str, String search, String replace) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Timestamp toTimestampTz(Timestamp timestamp, String fromUtc, String toUtc) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String atTimezone(Timestamp timestamp, String tz) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Timestamp dateParse(String str, String format) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long greatCircleDistance(
      Double originLat, Double originLng, Double destLat, Double DestLng) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public String dateFormat(Timestamp timestamp, String format) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long dateDiff(String unit, Timestamp from, Timestamp to) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public Long dateDiffString(String unit, String from, String to) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public boolean contains(List<String> collection, String unit) {
    throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
  }

  public static class ApproxPercentileAggregate {
    public Double init() {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double add(Double accumulator, Double val, Double percentile) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double merge(Double accumulator1, Double accumulator2) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double result(Double accumulator) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public static class MinByAggregate {
    public Double init() {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double add(Double accumulator, Object object, Double number) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double merge(Double accumulator1, Double accumulator2) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    public Double result(Double accumulator) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public static class CountDistinctAggregate implements CalciteAggregateUdf<Long, Object, Long> {

    @Override
    public Long init() {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    @Override
    public Long add(Long accumulator, Object val) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    @Override
    public Long merge(Long accumulator1, Long accumulator2) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }

    @Override
    public Long result(Long accumulator) {
      throw new UnsupportedOperationException(UDF_DECLARED_NOT_IMPLEMENTED_MESSAGE);
    }
  }
}

