package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jdk.nashorn.internal.objects.annotations.Getter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.query.conversion.dialect.HiveQLDialect;
import org.apache.calcite.sql.query.conversion.dialect.PrestoDialect;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum QueryEngine {
  PRESTO("presto", false, new PrestoDialect()),
  HIVE("hive", false, new HiveQLDialect());

  private static Map<String, QueryEngine> nameToQueryEngine =
      Arrays.stream(QueryEngine.values())
          .collect(
              Collectors.toMap(
                  queryEngine -> queryEngine.getName().toLowerCase(), Function.identity()));

  private final String name;
  private final boolean calciteOptimizationApplicable;
  @JsonIgnore
  private final SqlDialect dialect;

  QueryEngine(String name, boolean calciteOptimizationApplicable, SqlDialect dialect) {
    this.name = name;
    this.calciteOptimizationApplicable = calciteOptimizationApplicable;
    this.dialect = dialect;
  }

  public boolean shouldOptimizeQuery() {
    return calciteOptimizationApplicable;
  }

  @JsonCreator
  public static QueryEngine fromName(String name) {
    return nameToQueryEngine.get(name.toLowerCase());
  }

  public String getName() {
    return name;
  }

  public SqlDialect getDialect() {
    return dialect;
  }
}