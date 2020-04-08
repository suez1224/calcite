package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

public class PrestoToHiveUdfMapper {
  private final CaseInsensitiveMap<String, UdfConverter> udfConverterHashMap;
  private SqlHelper sqlHelper;

  public PrestoToHiveUdfMapper(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
    udfConverterHashMap =
        new CaseInsensitiveMap() {
          {
            put(
                UdfRegistry.PRESTO_TO_UNIXTIME.getName(),
                new UnixTimeUdfConverter(sqlHelper));
            put(
                UdfRegistry.PRESTO_NOW.getName(),
                new NowUdfConverter(sqlHelper)
            );
            put(
                UdfRegistry.PRESTO_DATE_DIFF.getName(),
                new DateDiffUdfConverter(sqlHelper));
            put(UdfRegistry.PRESTO_P50.getName(), new PercentileUdfConverter(sqlHelper));
            put(
                UdfRegistry.PRESTO_DATE_FORMAT.getName(),
                new DateFormatUdfConverter(sqlHelper));
          }
        };
  }

  public UdfConverter getUdfConverter(String prestoUdf) {
    return udfConverterHashMap.get(prestoUdf);
  }
}
