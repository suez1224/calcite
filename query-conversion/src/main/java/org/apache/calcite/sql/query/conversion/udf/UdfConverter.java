package org.apache.calcite.sql.query.conversion.udf;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.util.SqlShuttle;

public interface UdfConverter {
  SqlCall convert(SqlShuttle sqlShuttle, SqlCall sqlCall);
}

