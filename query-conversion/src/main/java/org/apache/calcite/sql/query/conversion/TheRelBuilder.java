package org.apache.calcite.sql.query.conversion;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;

public class TheRelBuilder extends RelBuilder {
  public TheRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public RelOptPlanner getPlanner() { return cluster.getPlanner(); }

  public RelOptCluster getCluster() { return cluster; }
}
