package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.calcite.sql.util.SqlShuttle;

import java.text.SimpleDateFormat;
import java.util.List;

public class UnixTimeUdfConverter implements UdfConverter {

  private SqlHelper sqlHelper;

  public UnixTimeUdfConverter(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
  }

  @Override
  public SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall) {
    /* convert
    to_unixtime(
        DATE_PARSE(
            REPLACE(substr(city_events.endTime, 1, 19), 'T', ' '),
            '%Y-%m-%d %H:%i:%s'
        )
    )
    to
    unix_timestamp(
            REPLACE(substr(city_events.endTime, 1, 19), 'T', ' '),
            'yyyy-MM-dd HH:mm:ss'
    )
    */
    List<SqlNode> operandList = sqlCall.getOperandList();
    if ((operandList.size() != 1) || !(operandList.get(0) instanceof SqlCall)) {
      throw new IllegalStateException("unexpected to_unixtime call syntax");
    }
    SqlCall operandCall = (SqlCall) operandList.get(0);
    if (operandCall.getKind() == SqlKind.OTHER_FUNCTION) {
      if (((SqlCall) operandList.get(0)).getOperator().getName().equalsIgnoreCase(
          UdfRegistry.PRESTO_DATE_PARSE.getName())) {
        operandList = operandCall.getOperandList();
        if (operandList.size() != 2) {
          throw new IllegalStateException("unexpected DATE_PARSE call syntax");
        }
        SqlLiteral format = (SqlLiteral) operandList.get(1);
        return sqlHelper.createUdfCall(
            UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(),
            operandList.get(0),
            SqlHelper.createLiteralString(convertTimeFormat(format.toValue())));
      } else {
        return sqlHelper.createUdfCall(
            UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(),
            operandCall.accept(sqlShuttle));
      }
    }
    throw new UnsupportedOperationException(
        String.format("Operator with %s and name %s NOT SUPPORTED",
            operandCall.getKind(), operandCall.getOperator().getName()));
  }

  public static String convertTimeFormat(String mysqlFormat) {
    String javeFormat =
        mysqlFormat
            .replace("%Y", "yyyy")
            .replace("%m", "MM")
            .replace("%d", "dd")
            .replace("%H", "HH")
            .replace("%i", "mm")
            .replace("%s", "ss");

    try {
      new SimpleDateFormat(javeFormat);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("converted date format is not valid", e);
    }

    return javeFormat;
  }
}

