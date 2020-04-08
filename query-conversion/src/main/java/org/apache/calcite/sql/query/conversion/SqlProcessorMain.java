package org.apache.calcite.sql.query.conversion;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.query.conversion.dialect.HiveQLDialect;
import org.apache.calcite.sql.query.conversion.udf.PrestoToHiveUdfMapper;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class SqlProcessorMain {

  public static void main(String[] args) throws Exception {
    InputStream is = SqlProcessorMain.class.getClassLoader().getResourceAsStream("test.txt");
//    String sqlString = IOUtils.toString(is, StandardCharsets.US_ASCII);
    String sqlString = //"select k, sum(h) as m from (select sum(b) as k, sum(c) as h, a from table1 group by a) group by k";
        "with state as (\n" +
            "  select\n" +
            "    *,\n" +
            "    Cast(\n" +
            "      DATE_DIFF('HOUR', from_unixtime(ts), now()) as VARCHAR\n" +
            "    ) || 'hr ' || Cast(\n" +
            "      MOD(DATE_DIFF('MINUTE', from_unixtime(ts), now()), 60) as VARCHAR\n" +
            "    ) || 'm' as state_since\n" +
            "  from\n" +
            "    rta.hp_co_rta_hp_bliss_agent_state_updatelogs_trimmed\n" +
            "  where\n" +
            "    agent_routing_mode = 'chat'\n" +
            "  limit\n" +
            "    1000\n" +
            ")\n" +
            "select\n" +
            "  agent.email,\n" +
            "  agent.name,\n" +
            "  state.current_availability as current_state,\n" +
            "  state.current_state_detail,\n" +
            "  agent.site_code,\n" +
            "  agent.site_name,\n" +
            "  state.state_since,\n" +
            "  state.agent_routing_mode\n" +
            "from\n" +
            "  state\n" +
            "  join dwh.dim_bliss_agent agent on state.agent_id = agent.uuid\n" +
            "LIMIT\n" +
            "  1000";

//    SqlHelper sqlHelper = new SqlHelper(SqlProcessorGlobal.frameworkConfig, SqlProcessorGlobal.calciteCatalogReader);
//    SqlNode sqlNode = new SqlProcessorMain().parseStmtAndHandleEx(sqlString, sqlHelper);
    Planner planner = Frameworks.getPlanner(SqlProcessorGlobal.frameworkConfig);
    SqlNode sqlNode = planner.parse(sqlString);
    SqlNode validated = planner.validate(sqlNode);
    RelRoot relRoot = planner.rel(validated);
    System.out.println("---------- Original Query ---------------");
    System.out.println(sqlString);
    System.out.println("---------- Rewritten Query ---------------");
    System.out.println(sqlNode.toSqlString(new HiveQLDialect()));
    System.out.println("---------- Rel ---------------");
    System.out.println(RelOptUtil.toString(relRoot.rel));
  }

  protected SqlNode parseStmtAndHandleEx(String sql, SqlHelper sqlHelper) {
    final SqlNode sqlNode;
    try {
      sqlNode = getSqlParser(sql).parseStmt();
      SqlNode rewritedNode = sqlNode.accept(new HiveQLShuttleVistor(new PrestoToHiveUdfMapper(sqlHelper)));
    } catch (SqlParseException e) {
      throw new RuntimeException("Error while parsing SQL: " + sql, e);
    }
    return sqlNode;
  }

  public SqlParser getSqlParser(String sql) {
    return getSqlParser(new SourceStringReader(sql));
  }

  protected SqlParser getSqlParser(Reader source) {
    return SqlParser.create(source, SqlProcessorGlobal.sqlParserConfig);
  }
}
