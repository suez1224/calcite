package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.sun.org.apache.xpath.internal.objects.XBoolean;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.query.conversion.udf.UdfRegistry;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;

public class SqlProcessorGlobal {
  private static SchemaPlus udfSchemaPlus = getUdfSchemaPlus();
  private static ObjectMapper objectMapper =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  private static DatabookManager databookManager = getDatabookManager();
  static SqlParser.Config sqlParserConfig =
      SqlParser.configBuilder()
        .setCaseSensitive(false)
        .setUnquotedCasing(Casing.UNCHANGED)
        .setQuotedCasing(Casing.UNCHANGED)
        .setConformance(SqlConformanceEnum.ORACLE_10)
        .build();
  private static SqlToRelConverter.Config sqlToRelConverterConfig =
      SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .build();

  static CalciteCatalogReader calciteCatalogReader =
      new CalciteCatalogReader(
          CalciteSchema.from(getSchemaPlus()), ImmutableList.of(), new JavaTypeFactoryImpl(), null);

  static FrameworkConfig frameworkConfig =
      Frameworks.newConfigBuilder()
          .parserConfig(sqlParserConfig)
          .defaultSchema(getSchemaPlus())
          .context(
              Contexts.of(
                  new CalciteConnectionConfigImpl(new Properties())
                      .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.toString(false))
                      .set(
                          CalciteConnectionProperty.CONFORMANCE,
                          SqlConformanceEnum.MYSQL_5.toString())))
          .operatorTable(
              ChainedSqlOperatorTable.of(ExtendedSqlOperatorTable.instance(), calciteCatalogReader))
          .traitDefs((List<RelTraitDef>) null)
          .sqlToRelConverterConfig(sqlToRelConverterConfig)
          .build();

  static SchemaPlus getSchemaPlus() {
    databookManager.registerTo(udfSchemaPlus);
    return udfSchemaPlus;
  }

  static SchemaPlus getUdfSchemaPlus() {
    if (udfSchemaPlus == null) {
      SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

      // TODO: Needs to consider more case-sensitive version maybe
      schemaPlus.add(
          UdfRegistry.PRESTO_FROM_ISO8601_TIMESTAMP.getName(),
          UdfRegistry.PRESTO_FROM_ISO8601_TIMESTAMP.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_NOW.getName(),
          UdfRegistry.PRESTO_NOW.getFunction()
      );
      schemaPlus.add(
          UdfRegistry.PRESTO_FROM_UNIXTIME.getName(), UdfRegistry.PRESTO_FROM_UNIXTIME.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_FROM_UNIXTIME.getName(), UdfRegistry.HIVE_FROM_UNIXTIME.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_TO_UNIXTIME.getName(), UdfRegistry.PRESTO_TO_UNIXTIME.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_SUBSTR.getName(), UdfRegistry.PRESTO_SUBSTR.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_REPLACE.getName(), UdfRegistry.PRESTO_REPLACE.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_DATE_PARSE.getName(), UdfRegistry.PRESTO_DATE_PARSE.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_DATE_FORMAT.getName(), UdfRegistry.PRESTO_DATE_FORMAT.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_DATE_DIFF.getName(), UdfRegistry.PRESTO_DATE_DIFF.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_DATE_DIFF_STRING.getName(),
          UdfRegistry.PRESTO_DATE_DIFF_STRING.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_GREAT_CIRCLE_DISTANCE.getName(),
          UdfRegistry.PRESTO_GREAT_CIRCLE_DISTANCE.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_AT_TIMEZONE.getName(), UdfRegistry.PRESTO_AT_TIMEZONE.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_P50.getName(), UdfRegistry.PRESTO_P50.getFunction());
      schemaPlus.add(UdfRegistry.HIVE_P50.getName(), UdfRegistry.PRESTO_P50.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_CONCAT.getName(), UdfRegistry.PRESTO_CONCAT.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_CONCAT.getName(), UdfRegistry.HIVE_CONCAT3.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_CONCAT.getName(), UdfRegistry.HIVE_CONCAT5.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_CONTAINS.getName(), UdfRegistry.PRESTO_CONTAINS.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_COUNT_DISTINCT.getName(),
          UdfRegistry.PRESTO_COUNT_DISTINCT.getFunction());
      schemaPlus.add(
          UdfRegistry.PRESTO_TO_TIMESTAMP_TZ.getName(),
          UdfRegistry.PRESTO_TO_TIMESTAMP_TZ.getFunction());

      schemaPlus.add(
          UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(), UdfRegistry.HIVE_UNIX_TIMESTAMP.getFunction());
      // n.b. overload unix_timestamp function in calcite
      schemaPlus.add(
          UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(),
          UdfRegistry.HIVE_UNIX_TIMESTAMP_FROM_TIMESTAMP.getFunction());
      schemaPlus.add(
          UdfRegistry.HIVE_UNIX_TIMESTAMP.getName(),
          UdfRegistry.HIVE_UNIX_TIMESTAMP_FROM_DATE.getFunction());
      schemaPlus.add(UdfRegistry.PRESTO_MIN_BY.getName(), UdfRegistry.PRESTO_MIN_BY.getFunction());
      schemaPlus.add(UdfRegistry.HIVE_DATE_ADD.getName(), UdfRegistry.HIVE_DATE_ADD.getFunction());
      schemaPlus.add(UdfRegistry.HIVE_DATE_SUB.getName(), UdfRegistry.HIVE_DATE_SUB.getFunction());
      schemaPlus.add(
          UdfRegistry.HIVE_DATE_TRUNC.getName(), UdfRegistry.HIVE_DATE_TRUNC.getFunction());
      schemaPlus.add(
          UdfRegistry.HIVE_GET_HEXAGON_ADDR.getName(),
          UdfRegistry.HIVE_GET_HEXAGON_ADDR.getFunction());
//      schemaPlus.add(
//          UdfRegistry.HIVE_FROM_UTC_TIMESTAMP.getName(),
//          UdfRegistry.HIVE_FROM_UTC_TIMESTAMP.getFunction());
      SchemaPlus esriShemaPlus = schemaPlus.add("esri", new AbstractSchema());
      esriShemaPlus.add(
          UdfRegistry.HIVE_ST_CONTAINS.getName(), UdfRegistry.HIVE_ST_CONTAINS.getFunction());
      esriShemaPlus.add(UdfRegistry.HIVE_ST_POINT.getName(), UdfRegistry.HIVE_ST_POINT.getFunction());
      esriShemaPlus.add(
          UdfRegistry.HIVE_ST_GEOM_FROM_TEXT.getName(),
          UdfRegistry.HIVE_ST_GEOM_FROM_TEXT.getFunction());
      udfSchemaPlus = schemaPlus;

      udfSchemaPlus.add("table1", new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.createStructType(
              StructKind.PEEK_FIELDS_DEFAULT,
              ImmutableList.of(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.INTEGER),
                  typeFactory.createSqlType(SqlTypeName.DOUBLE)),
              ImmutableList.of("a", "b", "c"));
        }
      });
      SchemaPlus sp = udfSchemaPlus.add("dwh", new AbstractSchema());
      sp.add("dim_bliss_agent", new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.createStructType(
              StructKind.PEEK_FIELDS_DEFAULT,
              ImmutableList.of(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR)),
              ImmutableList.of("email", "name", "site_code", "site_name", "uuid"));
        }
      });
      sp = udfSchemaPlus.add("rta", new AbstractSchema());
      sp.add("hp_co_rta_hp_bliss_agent_state_updatelogs_trimmed", new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.createStructType(
              StructKind.PEEK_FIELDS_DEFAULT,
              ImmutableList.of(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.BIGINT),
                  typeFactory.createSqlType(SqlTypeName.VARCHAR)),
              ImmutableList.of("agent_routing_mode", "current_availability", "current_state_detail", "ts", "agent_id"));
        }
      });
    }
    return udfSchemaPlus;
  }

  private static List<String> getSchemaFiles(String baseDir) throws IOException {
    final List<String> fileNames = new ArrayList<>();
//    Enumeration<JarEntry> entries =
//        new JarFile(
//            new File(SqlProcessorGlobal.class.getProtectionDomain().getCodeSource().getLocation().getPath()))
//            .entries();
//    while (entries.hasMoreElements()) {
//      JarEntry entry = entries.nextElement();
//      if (entry.getName().contains(baseDir) && entry.getName().endsWith(".json.gz")) {
//        String fileName = entry.getName().split("/")[entry.getName().split("/").length - 1];
//        fileNames.add(fileName);
//      }
//    }
    Arrays.stream(new File(SqlProcessorGlobal.class.getClassLoader().getResource(baseDir).getPath())
        .listFiles()).forEach(file -> fileNames.add(file.getName()));
    return fileNames;
  }

  private static DatabookManager getDatabookManager() {
    if (databookManager == null) {
      List<DatabookSchema> databookSchemaList = new ArrayList<>();
      /**
       * We need to use class loader because directly using File API based on relatively path will not
       * work when sql_processor module serves as a lib.
       */
      String baseDir = "databook_schemas";
      int i = 0;
      List<String> databookSchemaFileNames = new ArrayList<>();
      try {
        databookSchemaFileNames = getSchemaFiles(baseDir);
        for (; i < databookSchemaFileNames.size(); i++) {
          InputStream inputStream =
              SqlProcessorGlobal.class
                  .getClassLoader()
                  .getResourceAsStream(FilenameUtils.concat(baseDir, databookSchemaFileNames.get(i)));
          final String resp = IOUtils.toString(new GZIPInputStream(inputStream));
          DatabookSchema databookSchema = objectMapper.readValue(resp, DatabookSchema.class);
          databookSchemaList.add(databookSchema);
        }
      } catch (IOException | NullPointerException e) {
        throw new IllegalStateException(
            "Failed to read databook schema def " + databookSchemaFileNames.get(i), e);
      }

      databookManager = new DatabookManager(databookSchemaList);
    }
    return databookManager;
  }

  CalciteCatalogReader getCalciteCatalogReader(
      SchemaPlus schemaPlus, RelDataTypeFactory relDataTypeFactory) {
    CalciteCatalogReader calciteCatalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(schemaPlus), ImmutableList.of(), new JavaTypeFactoryImpl(), null);
    return calciteCatalogReader;
  }

  FrameworkConfig getFrameworkConfig(
      SchemaPlus schemaPlus,
      SqlParser.Config sqlParserConfig,
      SqlToRelConverter.Config sqlToRelConverterConfig,
      CalciteCatalogReader calciteCatalogReader) {
    return Frameworks.newConfigBuilder()
        .parserConfig(sqlParserConfig)
        .defaultSchema(schemaPlus)
        .context(
            Contexts.of(
                new CalciteConnectionConfigImpl(new Properties())
                    .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.toString(false))
                    .set(
                        CalciteConnectionProperty.CONFORMANCE,
                        SqlConformanceEnum.MYSQL_5.toString())))
        .operatorTable(
            ChainedSqlOperatorTable.of(SqlOperatorTable.instance(), calciteCatalogReader))
        .traitDefs((List<RelTraitDef>) null)
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .build();
  }

  Planner getPlanner(FrameworkConfig frameworkConfig) {
    return Frameworks.getPlanner(frameworkConfig);
  }
}
