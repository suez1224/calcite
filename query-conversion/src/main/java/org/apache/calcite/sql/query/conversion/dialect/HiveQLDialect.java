package org.apache.calcite.sql.query.conversion.dialect;

import com.google.common.base.Preconditions;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.query.conversion.udf.UdfRegistry;

import java.util.List;

public class HiveQLDialect extends HiveSqlDialect {

  public HiveQLDialect() {
    super(EMPTY_CONTEXT.withIdentifierQuoteString("`"));
  }

  @Override
  protected boolean allowsAs() {
    return true;
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    switch (call.getKind()) {
      case OTHER_FUNCTION:
        SqlOperator operator = call.getOperator();
        // Identify the pattern to_timestamp_tz(date_parse(xxx), "UTC", timezone) and convert to
        // from_utc_timestamp(xxx, timezone)
        if (UdfRegistry.PRESTO_TO_TIMESTAMP_TZ.getName().equalsIgnoreCase(operator.getName())) {
          Preconditions.checkState(call.getOperandList().size() == 3);

          if (call.getOperandList().get(0).getKind() == SqlKind.OTHER_FUNCTION
              && call.getOperandList().get(1).getKind() == SqlKind.LITERAL) {
            SqlCall firstOperand = (SqlCall) call.getOperandList().get(0);
            SqlLiteral secondOperand = (SqlLiteral) call.getOperandList().get(1);
            SqlNode thirdOperand = call.getOperandList().get(2);

            if (UdfRegistry.PRESTO_DATE_PARSE
                .getName()
                .equalsIgnoreCase(firstOperand.getOperator().getName())
                && "utc".equalsIgnoreCase(secondOperand.getValueAs(String.class))) {

              /**
               * TODO(Miao): Currently assume here is only DATE_PARSE(xxx, format) since this is the
               * only timestamp parse function we are supporting. In the future, we should migrate
               * conversion date_parse to Hive here. And format should be converted individually in
               * visitor if needed.
               */
              Preconditions.checkState(
                  "date_parse".equalsIgnoreCase(firstOperand.getOperator().getName())
                      && firstOperand.getOperandList().size() == 2);

              writer.keyword("from_utc_timestamp");
              SqlWriter.Frame fromUtcTimestampFrame =
                  writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
              firstOperand.getOperandList().get(0).unparse(writer, 0, 0);
              writer.sep(",");
              thirdOperand.unparse(writer, 0, 0);
              writer.endList(fromUtcTimestampFrame);
              return;
            }
          }
        }

        super.unparseCall(writer, call, leftPrec, rightPrec);
        break;
      case EXTRACT:
        SqlNode firstOperator = call.getOperandList().get(0);
        // Identify the pattern EXTRACT(DOW from XXX) and convert it to
        // from_unixtime(unix_timestamp(xxx), "u")
        if (call.getOperandList().size() == 2 && firstOperator instanceof SqlIntervalQualifier) {
          SqlIntervalQualifier qualifier = (SqlIntervalQualifier) firstOperator;
          if (qualifier.timeUnitRange == TimeUnitRange.DOW) {
            // Start from_unixtime
            writer.keyword("from_unixtime");
            SqlWriter.Frame fromUnixtimeFrame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");

            // Start unix_timestamp
            writer.keyword("unix_timestamp");
            SqlWriter.Frame unixTimestampFrame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");

            call.getOperandList().get(1).unparse(writer, 0, 0);

            writer.endList(unixTimestampFrame);
            writer.sep(",");
            SqlLiteral.createCharString("u", SqlParserPos.ZERO).unparse(writer, 0, 0);
            writer.endList(fromUnixtimeFrame);
            return;
          }
        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
        break;
      case MAP_VALUE_CONSTRUCTOR:
        writer.keyword(call.getOperator().getName());
        final SqlWriter.Frame frame = writer.startList("(", ")");
        List<SqlNode> nodes = call.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
          if (i != nodes.size() - 1) {
            writer.keyword(nodes.get(i).toString() + ",");
          } else {
            writer.keyword(nodes.get(i).toString());
          }
        }
        writer.endList(frame);
        break;
      case JOIN:
        SqlJoin join = (SqlJoin) call;
        SqlNode right = join.getRight();

        // Identify pattern like:
        // table_name CROSS JOIN UNNEST(table_name.array_columns) AS tmp_table(column)
        // Convert it to:
        // table name LATERAL VIEW EXPLODE(table_name.array_columns) tmp_table AS column
        if (join.getJoinType() == JoinType.CROSS
            && join.getCondition() == null
            && right.getKind() == SqlKind.AS) {
          SqlCall as = (SqlCall) right;
          SqlNode asLeft = as.getOperandList().get(0);
          if (asLeft.getKind() == SqlKind.UNNEST) {
            Preconditions.checkState(as.getOperandList().size() == 3);
            SqlCall asLeftCall = (SqlCall) asLeft;
            SqlWriter.Frame joinFrame = writer.startList(SqlWriter.FrameTypeEnum.JOIN);
            join.getLeft().unparse(writer, leftPrec, join.getOperator().getLeftPrec());

            // Convert CROSS JOIN to LATERAL VIEW
            writer.keyword("LATERAL VIEW");

            // Convert UNNEST() to explode()
            writer.keyword("explode");
            SqlWriter.Frame innerFrame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            for (SqlNode sqlNode : asLeftCall.getOperandList()) {
              writer.sep(",");
              sqlNode.unparse(writer, 0, 0);
            }
            writer.endList(innerFrame);

            as.getOperandList().get(1).unparse(writer, 0, 0);
            // Handle as
            writer.sep("AS");
            as.getOperandList().get(2).unparse(writer, 0, 0);
            writer.endList(joinFrame);
            return;
          }
        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
        break;
      case WITH:
        final SqlWith with = (SqlWith) call;
        final SqlWriter.Frame withFrame =
            writer.startList(SqlWriter.FrameTypeEnum.WITH, "WITH", "");
        final SqlWriter.Frame frame1 = writer.startList("", "");
        for (SqlNode node : with.withList) {
          writer.sep(",");
          node.unparse(writer, 0, 0);
        }
        writer.endList(frame1);
        final SqlWriter.Frame frame2 = writer.startList(SqlWriter.FrameTypeEnum.WITH);
        with.body.unparse(writer, 100, 100);
        writer.endList(frame2);
        writer.endList(withFrame);
        break;
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override
  public boolean identifierNeedsToBeQuoted(String identifier) {
    if (identifier.contains("$")) {
      return true;
    }
    return PrestoDialect.CALCITE_LOWER_CASE_RESERVED_WORDS.contains(identifier.toLowerCase());
  }
}
