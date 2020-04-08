package org.apache.calcite.sql.query.conversion;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class ExtendedSqlOperatorTable extends SqlStdOperatorTable {
  public static final SqlBinaryOperator NULL_EQUALS =
      new SqlBinaryOperator(
          "<=>",
          SqlKind.EQUALS,
          30,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED);

  /** This operator is only for HIVE because normal `/` will not truncate for integers in Hive. */
  public static final SqlBinaryOperator HIVE_INTEGER_DIV =
      new SqlBinaryOperator(
          "DIV",
          SqlKind.DIVIDE,
          60,
          true,
          ReturnTypes.QUOTIENT_NULLABLE,
          InferTypes.FIRST_KNOWN,
          OperandTypes.DIVISION_OPERATOR);

  private static ExtendedSqlOperatorTable instance;

  public static synchronized SqlStdOperatorTable instance() {
    if (instance == null) {
      instance = new ExtendedSqlOperatorTable();
      instance.init();
    }

    return instance;
  }
}
