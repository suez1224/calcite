package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class UdfInfo {
  private final String name;
  private final SqlIdentifier sqlIdentifier;
  private final Function function;

  public UdfInfo(String name, Function function) {
    this.name = name;
    try {
      this.sqlIdentifier = (SqlIdentifier) SqlParser.create("\"" + name + "\"").parseExpression();
    } catch (SqlParseException e) {
      throw new IllegalArgumentException(e);
    }
    this.function = function;
  }

  public String getName() {
    return name;
  }

  public SqlIdentifier getSqlIdentifier() {
    return sqlIdentifier;
  }

  public Function getFunction() {
    return function;
  }
}
