package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.sources.TableSource;

/**
 *
 */
public class HiveCatalogConnectorCatalogTableTest {

//	@Override
	public CatalogTable createTable() {
		return ConnectorCatalogTable.source(new TableSource<String>() {
			@Override
			public TableSchema getTableSchema() {
				return TableSchema.builder()
					.field("k1", DataTypes.STRING())
					.build();
			}
		}, true);
	}

//	@Override
	public CatalogTable createAnotherTable() {
		return ConnectorCatalogTable.source(new TableSource<Integer>() {
			@Override
			public TableSchema getTableSchema() {
				return TableSchema.builder()
					.field("k2", DataTypes.INT())
					.build();
			}
		}, false);
	}

}
