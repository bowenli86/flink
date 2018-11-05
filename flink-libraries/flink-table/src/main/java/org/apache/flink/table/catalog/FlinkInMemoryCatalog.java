/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory production implementation of {@link ExternalCatalog} for production use.
 */
public class FlinkInMemoryCatalog implements CrudExternalCatalog {
	final String catalogName;

	protected final Map<String, ExternalCatalogTable> tables = new ConcurrentHashMap<>();

	public FlinkInMemoryCatalog(String catalogName) {
		this.catalogName = catalogName;
	}

	@Override
	public void createTable(String tableName, ExternalCatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException {
		if (tables.containsKey(tableName)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tableName);
			}
		} else {
			tables.put(tableName, table);
		}
	}

	@Override
	public void dropTable(String tableName, boolean ignoreIfNotExists)
		throws TableNotExistException {
		if (tables.remove(tableName) == null && !ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName);
		}
	}

	@Override
	public void alterTable(String tableName, ExternalCatalogTable table, boolean ignoreIfNotExists)
		throws TableNotExistException {
		if (tables.containsKey(tableName)) {
			tables.put(tableName, table);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName);
		}
	}

	@Override
	public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
		ExternalCatalogTable result = tables.get(tableName);
		if (result == null) {
			throw new TableNotExistException(catalogName, tableName, null);
		} else {
			return result;
		}
	}

	@Override
	public List<String> listTables() {
		return new ArrayList<>(tables.keySet());
	}

	// ------ FlinkInMemoryCatalog doesn't support sub-catalog -------

	@Override
	public void createSubCatalog(String name, ExternalCatalog catalog, boolean ignoreIfExists)
		throws CatalogAlreadyExistException {
		throw new UnsupportedOperationException("FlinkInMemoryCatalog doesn't support sub-catalog");
	}

	@Override
	public void dropSubCatalog(String name, boolean ignoreIfNotExists)
		throws CatalogNotExistException {
		throw new UnsupportedOperationException("FlinkInMemoryCatalog doesn't support sub-catalog");
	}

	@Override
	public void alterSubCatalog(String name, ExternalCatalog catalog, boolean ignoreIfNotExists)
		throws CatalogNotExistException {
		throw new UnsupportedOperationException("FlinkInMemoryCatalog doesn't support sub-catalog");
	}

	@Override
	public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
		throw new UnsupportedOperationException("FlinkInMemoryCatalog doesn't support sub-catalog");
	}

	@Override
	public List<String> listSubCatalogs() {
		throw new UnsupportedOperationException("FlinkInMemoryCatalog doesn't support sub-catalog");
	}
}
