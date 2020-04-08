package org.apache.calcite.sql.query.conversion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;

public class DatabookManager {
  private final List<DatabookSchema> databookSchemaList;

  public DatabookManager(List<DatabookSchema> databookSchemaList) {
    this.databookSchemaList = databookSchemaList;
  }

  public Map<String, Map<String, Table>> getDbToTableNameToTable() {
    Map<String, Map<String, Table>> dbToTableNameToTable = new HashMap<>();
    for (DatabookSchema databookSchema : databookSchemaList) {
      Map<String, Table> tableNameToTable =
          dbToTableNameToTable.getOrDefault(databookSchema.getDatabase(), new HashMap<>());
      tableNameToTable.put(databookSchema.getTableName(), new DatabookTable(databookSchema));
      dbToTableNameToTable.put(databookSchema.getDatabase(), tableNameToTable);
    }
    return dbToTableNameToTable;
  }

  public void registerTo(SchemaPlus schemaPlus) {
    Map<String, Map<String, Table>> dbToTableNameToTable = getDbToTableNameToTable();

    for (Map.Entry<String, Map<String, Table>> entry : dbToTableNameToTable.entrySet()) {
      String dbName = entry.getKey();
      Map<String, Table> tableNameToTable = entry.getValue();

      Map<String, Table> allTableNameToTable = new HashMap<>();

      // Get all existing table map
      SchemaPlus subSchema = schemaPlus.getSubSchema(dbName);
      if (subSchema != null) {
        for (String tableName : subSchema.getTableNames()) {
          allTableNameToTable.put(tableName, subSchema.getTable(tableName));
        }
      }
      allTableNameToTable.putAll(tableNameToTable);

      schemaPlus.add(
          dbName,
          new AbstractSchema() {
            @Override
            public Map<String, Table> getTableMap() {
              return allTableNameToTable;
            }
          });
    }
  }

  public boolean containsTable(String table) {
    Map<String, Map<String, Table>> dbToTableNameToTable = getDbToTableNameToTable();
    String[] path = table.split("\\.");
    return path.length == 2
        && dbToTableNameToTable.containsKey(path[0])
        && dbToTableNameToTable.get(path[0]).containsKey(path[1]);
  }

  private static final class DatabookTable extends AbstractTable {
    private final DatabookSchema databookSchema;

    private DatabookTable(DatabookSchema databookSchema) {
      this.databookSchema = databookSchema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      return databookSchema.convertToRelDataType(relDataTypeFactory);
    }
  }
}
