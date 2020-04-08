package org.apache.calcite.sql.query.conversion;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.Properties;

public class SqlDecompositionMain {
  public static void main(String[] args) throws Exception {
    FrameworkConfig frameworkConfig = SqlProcessorGlobal.frameworkConfig;
    VolcanoPlanner planner = new VolcanoPlanner(Contexts.empty());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    Properties prop = new Properties();
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(SqlProcessorGlobal.sqlParserConfig.caseSensitive()));
    CalciteCatalogReader relOptSchema = new CalciteCatalogReader(
        CalciteSchema.from(SqlProcessorGlobal.getSchemaPlus()),
        Collections.emptyList(),
        typeFactory,
        new CalciteConnectionConfigImpl(prop));
    TheRelBuilder relBuilder = new TheRelBuilder(frameworkConfig.getContext(), cluster, relOptSchema);

  }
}
