package org.apache.calcite.sql.query.conversion.dialect;

import org.apache.calcite.sql.SqlDialect;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.query.conversion.ExtendedSqlOperatorTable;


public class PrestoDialect extends SqlDialect {
  public static Set<String> CALCITE_LOWER_CASE_RESERVED_WORDS =
      SqlAbstractParserImpl.getSql92ReservedWords()
          .stream()
          .map(String::toLowerCase)
          .collect(Collectors.toSet());

  public PrestoDialect() {
    super(EMPTY_CONTEXT.withIdentifierQuoteString("\""));
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR) {
      writer.keyword(call.getOperator().getName());
      final SqlWriter.Frame frame = writer.startList("(", ")");
      Preconditions.checkArgument(call.getOperandList().size() % 2 == 0);

      boolean onKey = true;
      List<SqlNode> keySqlNodeList = new ArrayList<>(call.getOperandList().size() / 2);
      List<SqlNode> valueSqlNodeList = new ArrayList<>(call.getOperandList().size() / 2);
      for (SqlNode operand : call.getOperandList()) {
        if (onKey) {
          keySqlNodeList.add(operand);
        } else {
          valueSqlNodeList.add(operand);
        }

        onKey = !onKey;
      }

      unparseSqlNodeListAsArrayValue(writer, keySqlNodeList, leftPrec, rightPrec);
      unparseSqlNodeListAsArrayValue(writer, valueSqlNodeList, leftPrec, rightPrec);
      writer.endList(frame);
      return;
    }

    if (call.getKind() == SqlKind.EQUALS
        && call.getOperator().getName().equals(ExtendedSqlOperatorTable.NULL_EQUALS.getName())) {
      assert call.operandCount() == 2;
      SqlOperator operator = call.getOperator();
      final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
      call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec());
      final boolean needsSpace = true;
      writer.setNeedWhitespace(needsSpace);
      writer.sep("=");
      writer.setNeedWhitespace(needsSpace);
      call.operand(1).unparse(writer, operator.getRightPrec(), rightPrec);
      writer.endList(frame);
      return;
    }

    if (call.getKind() == SqlKind.DIVIDE) {
      SqlUtil.unparseBinarySyntax(SqlStdOperatorTable.DIVIDE, call, writer, leftPrec, rightPrec);
      return;
    }

    call.getOperator().unparse(writer, call, leftPrec, rightPrec);
  }

  private void unparseSqlNodeListAsArrayValue(
      SqlWriter writer, List<SqlNode> sqlNodeList, int leftPrec, int rightPrec) {
    SqlCall arrayValue =
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
            new SqlNodeList(sqlNodeList, SqlParserPos.ZERO));
    writer.sep(",");
    arrayValue.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public boolean identifierNeedsToBeQuoted(String identifier) {
    if (identifier.contains("$")) {
      return true;
    }
    return CALCITE_LOWER_CASE_RESERVED_WORDS.contains(identifier.toLowerCase());
  }
}

