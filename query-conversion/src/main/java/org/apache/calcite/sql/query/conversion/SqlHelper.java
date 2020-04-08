package org.apache.calcite.sql.query.conversion;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.Litmus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SqlHelper {
  public static final String METRIC_RESULT_TABLE_POSTFIX = "_table";
  public static SqlParser.Config SQL_PARSER_CONFIG =
      SqlParser.configBuilder()
          .setCaseSensitive(false)
          .setUnquotedCasing(Casing.UNCHANGED)
          .setQuotedCasing(Casing.UNCHANGED)
          .setConformance(SqlConformanceEnum.ORACLE_10)
          .build();
  public static final SqlDataTypeSpec TYPE_SPEC_BIGINT =
      new SqlDataTypeSpec(
          new SqlIdentifier("BIGINT", SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO);
  public static final SqlDataTypeSpec TYPE_SPEC_INT =
      new SqlDataTypeSpec(
          new SqlIdentifier("INTEGER", SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO);
  public static final SqlDataTypeSpec TYPE_SPEC_VARCHAR =
      new SqlDataTypeSpec(
          new SqlIdentifier("VARCHAR", SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO);

  private static final Logger logger = LoggerFactory.getLogger(SqlHelper.class);
  private final FrameworkConfig frameworkConfig;
  private final CalciteCatalogReader calciteCatalogReader;

  SqlHelper(FrameworkConfig frameworkConfig, CalciteCatalogReader calciteCatalogReader) {
    this.frameworkConfig = frameworkConfig;
    this.calciteCatalogReader = calciteCatalogReader;
  }

  public static String getSqlString(SqlNode sqlNode, QueryEngine queryEngine) {
    SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(queryEngine.getDialect());
    sqlPrettyWriter.setQuoteAllIdentifiers(false);
    sqlNode.unparse(sqlPrettyWriter, 0, 0);
    String sqlString = sqlPrettyWriter.toSqlString().toString();
    return sqlString;
  }

  public static SqlNode getCastToString(SqlNode sqlNode) {
    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, sqlNode, TYPE_SPEC_VARCHAR);
  }

  public static SqlNode getCast(SqlNode sqlNode, SqlDataTypeSpec sqlDataTypeSpec) {
    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, sqlNode, sqlDataTypeSpec);
  }

  public SqlCall createUdfCall(String udfName, SqlNode... sqlNodes) {
    List<SqlOperator> sqlOperators = new ArrayList<>();
    frameworkConfig
        .getOperatorTable()
        .lookupOperatorOverloads(
            createSqlIdentifier(udfName),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION,
            sqlOperators);
    SqlOperator sqlOperator = sqlOperators.get(0);
    return sqlOperator.createCall(SqlParserPos.ZERO, sqlNodes);
  }

  public static SqlNode createLiteralNumber(long value) {
    return SqlLiteral.createExactNumeric(String.valueOf(value), SqlParserPos.ZERO);
  }

  public static SqlNode createLiteralBoolean(boolean value) {
    return SqlLiteral.createBoolean(value, SqlParserPos.ZERO);
  }

  public static SqlNode createLiteralString(String value) {
    return SqlLiteral.createCharString(value, SqlParserPos.ZERO);
  }

  public static SqlCall createCall(SqlOperator sqlOperator, List<SqlNode> sqlNodeList) {
    return sqlOperator.createCall(createSqlNodeList(sqlNodeList));
  }

  public static SqlNode createAndCall(Iterable<SqlNode> sqlNodeList) {
    Iterator<SqlNode> iter = sqlNodeList.iterator();
    SqlNode call = iter.next();
    while (iter.hasNext()) {
      call = createCall(SqlStdOperatorTable.AND, ImmutableList.of(call, iter.next()));
    }
    return call;
  }

  public static boolean sqlNodeEqual(SqlNode node1, SqlNode node2) {
    return Objects.equals(node1, node2)
        || (node1 != null && node2 != null && node1.equalsDeep(node2, Litmus.IGNORE));
  }

  public static SqlIdentifier createSqlIdentifier(String... path) {
    return new SqlIdentifier(Arrays.asList(path), SqlParserPos.ZERO);
  }

  public static SqlNodeList createSqlNodeList(Collection<SqlNode> sqlNodeList) {
    return new SqlNodeList(sqlNodeList, SqlParserPos.ZERO);
  }

  public static SqlNode parseMetricToSqlNode(String query) throws SqlParseException {
    try {
      String expression = "(" + query + ")";
      SqlParser sqlParser = SqlParser.create(expression, SQL_PARSER_CONFIG);
      return sqlParser.parseExpression();
    } catch (SqlParseException exception) {
      logger.warn("Failed to parse " + query);
      return SqlHelper.parseStatement(query);
    }
  }

  private static SqlNode parseStatement(String query) throws SqlParseException {
    SqlParser sqlParser = SqlParser.create(query);
    return sqlParser.parseQuery();
  }

  public static String getViewName(SqlSelect sqlSelect) {
    if (sqlSelect.getFrom().getKind().equals(SqlKind.JOIN)) {
      SqlJoin join = (SqlJoin) sqlSelect.getFrom();
      while (join.getLeft().getKind().equals(SqlKind.JOIN)) {
        join = (SqlJoin) join.getLeft();
      }
      return join.getLeft().toString();
    }
    return sqlSelect.getFrom().toString();
  }

  public static String getTmpTableName(String metricName) {
    return metricName + METRIC_RESULT_TABLE_POSTFIX;
  }
}