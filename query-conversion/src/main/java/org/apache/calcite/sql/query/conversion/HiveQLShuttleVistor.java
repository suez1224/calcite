package org.apache.calcite.sql.query.conversion;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.query.conversion.udf.PrestoToHiveUdfMapper;
import org.apache.calcite.sql.query.conversion.udf.UdfConverter;
import org.apache.calcite.sql.util.SqlShuttle;

// Use to traverse AST of SqlNode and convert Presto syntax to Hive syntax.
public class HiveQLShuttleVistor extends SqlShuttle {

  private final PrestoToHiveUdfMapper udfMapper;
  private final ImmutableMap<String, SqlDataTypeSpec> prestoToHiveTypeMap;

  public HiveQLShuttleVistor(PrestoToHiveUdfMapper mapper) {
    this.udfMapper = mapper;
    prestoToHiveTypeMap = buildTypeMap();
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call.getKind().equals(SqlKind.OTHER_FUNCTION)) {
      String udfName = call.getOperator().getName();
      UdfConverter converter = udfMapper.getUdfConverter(udfName);
      if (converter != null) {
        call = converter.convert(this, call);
      }
    }

    return deepCloneSqlCall(call);
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    return prestoToHiveTypeMap.getOrDefault(type.getTypeName().getSimple(), type);
  }

  private SqlNode deepCloneSqlCall(SqlCall call) {
    ArgHandler<SqlNode> argHandler = new SqlShuttle.CallCopyingArgHandler(call, false);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }

  private ImmutableMap<String, SqlDataTypeSpec> buildTypeMap() {
    return ImmutableMap.of(
        "VARCHAR",
        new SqlDataTypeSpec(
            new SqlIdentifier("STRING", SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO));
  }
}
