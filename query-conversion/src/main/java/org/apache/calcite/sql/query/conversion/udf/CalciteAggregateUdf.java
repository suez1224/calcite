package org.apache.calcite.sql.query.conversion.udf;

/** A generic interface for Calcite Aggregate UDF */
public interface CalciteAggregateUdf<A, V, R> {
  A init();

  A add(A accumulator, V val);

  A merge(A accumulator1, A accumulator2);

  R result(A accumulator);
}

