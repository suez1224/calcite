package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.calcite.sql.util.SqlShuttle;

public class NowUdfConverter implements UdfConverter {
  private SqlHelper sqlHelper;

  public NowUdfConverter(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
  }
  @Override
  public SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall) {
    return sqlHelper.createUdfCall(UdfRegistry.HIVE_CURRENT_TIMESTAMP.getName());
  }
}
