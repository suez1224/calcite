/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.babel;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by suez on 10/14/19.
 */
public class BabelMain {
  Quoting quoting = Quoting.DOUBLE_QUOTE;
  Casing unquotedCasing = Casing.TO_UPPER;
  Casing quotedCasing = Casing.UNCHANGED;
  SqlConformance conformance = SqlConformanceEnum.DEFAULT;

  public static void main(String[] args) throws Exception {
    InputStream is = BabelMain.class.getClassLoader().getResourceAsStream("test.txt");
    BufferedReader br = new BufferedReader(new InputStreamReader(is));

    String line = null;
    int count = 1;
    while ((line = br.readLine()) != null) {
      String query = line
          .replaceAll("^\\|+", "")
          .replaceAll("\\|+$", "");
      SqlNode sqlNode = new BabelMain().parseStmtAndHandleEx(query);
      System.out.println(count++ + "-------------------------");
      System.out.println(sqlNode.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()));
    }
  }

  protected SqlNode parseStmtAndHandleEx(String sql) {
    final SqlNode sqlNode;
    try {
      sqlNode = getSqlParser(sql).parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("Error while parsing SQL: " + sql, e);
    }
    return sqlNode;
  }

  protected SqlParserImplFactory parserImplFactory() {
    return SqlParserImpl.FACTORY;
  }

  public SqlParser getSqlParser(String sql) {
    return getSqlParser(new SourceStringReader(sql));
  }

  protected SqlParser getSqlParser(Reader source) {
    return SqlParser.create(source,
        SqlParser.configBuilder()
            .setParserFactory(parserImplFactory())
            .setQuoting(quoting)
            .setUnquotedCasing(unquotedCasing)
            .setQuotedCasing(quotedCasing)
            .setConformance(conformance)
            .build());
  }
}
// End BabelMain.java
