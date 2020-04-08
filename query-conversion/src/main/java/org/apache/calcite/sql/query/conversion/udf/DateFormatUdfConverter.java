package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.query.conversion.SqlHelper;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.List;

public class DateFormatUdfConverter implements UdfConverter {

  private SqlHelper sqlHelper;

  public DateFormatUdfConverter(SqlHelper sqlHelper) {
    this.sqlHelper = sqlHelper;
  }

  @Override
  public SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall) {
    /* convert
    date_format(timestamp, '%Y-%m-%d')

    to

    date_format(timestamp, 'yyyy-MM-dd')
    */
    List<SqlNode> operandList = sqlCall.getOperandList();
    if (operandList.size() != 2 || !(operandList.get(1) instanceof SqlLiteral)) {
      throw new IllegalStateException("unexpected date_format call operands");
    }

    String prestoFormat = operandList.get(1).toString();
    String hiveFormat = UnixTimeUdfConverter.convertTimeFormat(prestoFormat);
    if (operandList.get(0).getKind() == SqlKind.OTHER_FUNCTION) {
      sqlCall.setOperand(0, operandList.get(0).accept(sqlShuttle));
    }
    sqlCall.setOperand(1, SqlHelper.createLiteralString(hiveFormat));
    return sqlCall;
  }
}
