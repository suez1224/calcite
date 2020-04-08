package org.apache.calcite.sql.query.conversion.udf;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;

import java.util.List;

public class DateDiffUdfConverter implements UdfConverter {
  private SqlHelper sqlHelper;

  public DateDiffUdfConverter(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
  }

  @Override
  public SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall) {
    /* convert
    DATE_DIFF(
      'second',
      DATE_PARSE(
        REPLACE(
          substr(demand_jobs.assigned.`time`, 1, 19), 'T', ' '),
        '%Y-%m-%d %H:%i:%s'
      ),
      DATE_PARSE(
        REPLACE(
          substr(demand_jobs.pickup_arrived.`time`, 1, 19), 'T', ' '),
        '%Y-%m-%d %H:%i:%s'
      )
    )

    to

    unix_timestamp(
      REPLACE(
        substr(demand_jobs.pickup_arrived.`time`, 1, 19), 'T', ' '),
      'yyyy-MM-dd HH:mm:ss'
    ) - unix_timestamp(
      REPLACE(
        substr(demand_jobs.assigned.`time`, 1, 19), 'T', ' '),
      'yyyy-MM-dd HH:mm:ss'
    )
    */
    List<SqlNode> operandList = sqlCall.getOperandList();
    if (operandList.size() != 3 || !(operandList.get(0) instanceof SqlLiteral)) {
      throw new IllegalStateException("unexpected date_diff call operands");
    }

    SqlLiteral sqlLiteral = (SqlLiteral) operandList.get(0);
    if (!((NlsString) sqlLiteral.getValue()).getValue().equals("second")) {
      throw new IllegalStateException("unexpected date_diff syntax");
    }

    SqlNode timestamp1 = createUnixtimeCall((SqlCall) operandList.get(1));
    SqlNode timestamp2 = createUnixtimeCall((SqlCall) operandList.get(2));
    return SqlHelper.createCall(
        SqlStdOperatorTable.MINUS, ImmutableList.of(timestamp2, timestamp1));
  }

  private SqlCall createUnixtimeCall(SqlCall dateParseCall) {
    SqlOperator dateParseOperator = dateParseCall.getOperator();

    if (!dateParseOperator.getKind().equals(SqlKind.OTHER_FUNCTION)
        || !dateParseOperator.getName().equals(UdfRegistry.PRESTO_DATE_PARSE.getName())) {
      throw new IllegalStateException("unexpected date_diff call syntax");
    }

    List<SqlNode> operandList = dateParseCall.getOperandList();
    if (operandList.size() != 2) {
      throw new IllegalStateException("unexpected date_diff call syntax");
    }

    SqlLiteral format = (SqlLiteral) operandList.get(1);
    return sqlHelper.createUdfCall(
        UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(),
        operandList.get(0),
        SqlHelper.createLiteralString(
            UnixTimeUdfConverter.convertTimeFormat(format.toValue())));
  }
}
