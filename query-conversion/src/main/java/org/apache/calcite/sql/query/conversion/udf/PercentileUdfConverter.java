package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.List;

public class PercentileUdfConverter implements UdfConverter {
  private SqlHelper sqlHelper;

  public PercentileUdfConverter(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
  }

  @Override
  public SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall) {
    /* convert
    APPROX_PERCENTILE(demand_jobs.assigned.predicted_eta, 0.90) / 60.0 AS p90_eta_post_request,
    to
    PERCENTILE_APPROX(demand_jobs.assigned.predicted_eta, 0.90) / 60.0 AS p90_eta_post_request,
    */
    List<SqlNode> operands = sqlCall.getOperandList();
    return sqlHelper.createUdfCall(
        UdfRegistry.HIVE_P50.getName(), operands.toArray(new SqlNode[operands.size()]));
  }
}