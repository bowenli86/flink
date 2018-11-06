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

import org.apache.flink.table.api.ExternalCatalogAlreadyExistException;
import org.apache.flink.table.api.ExternalCatalogNotExistException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.schema.RelTable;
import org.apache.flink.table.utils.TableTestBase;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for CatalogManager.
 */
public class CatalogManagerTest extends TableTestBase {

	private CatalogManager catalogManager = new CatalogManager(streamTestUtil().tableEnv());

	private static final String TEST_CATALOG = "test_catalog";
	private static final String TEST_TABLE = "test_table";

	private ExternalCatalog testCatalog = new InMemoryExternalCatalog(TEST_CATALOG);
	private AbstractTable testTable = new RelTable(null);

	@Before
	public void init() {
		catalogManager.registerExternalCatalog(TEST_CATALOG, testCatalog);
		catalogManager.registerTable(TEST_TABLE, testTable);
	}

	// ------ catalogs ------

	@Test(expected = ExternalCatalogAlreadyExistException.class)
	public void testRegisterAlreadyExistCatalog() {
		catalogManager.registerExternalCatalog(TEST_CATALOG, testCatalog);
	}

	@Test
	public void testGetRegisteredCatalog() {
		assertEquals(testCatalog, catalogManager.getExternalCatalog(TEST_CATALOG));
	}

	@Test(expected = ExternalCatalogNotExistException.class)
	public void testGetNotExistRegisteredCatalog() {
		catalogManager.getExternalCatalog("missing");
	}

	@Test
	public void testGetExternalCatalogs() {
		assertEquals(
			Arrays.asList(CatalogManager.DEFAULT_FLINK_DEFAULT_CATALOG_NAME, TEST_CATALOG),
			catalogManager.getExternalCatalogs());
	}

	@Test
	public void testSetDefaultCatalog() {
		assertEquals(
			catalogManager.getExternalCatalog(CatalogManager.DEFAULT_FLINK_DEFAULT_CATALOG_NAME),
			catalogManager.getDefaultCatalog());

		catalogManager.setDefaultCatalog(TEST_CATALOG);

		assertEquals(testCatalog, catalogManager.getDefaultCatalog());
	}

	// ------ tables ------

	@Test
	public void testListTables() {
		assertEquals(Arrays.asList(TEST_TABLE), catalogManager.listTables());
	}

	@Test
	public void testRegisterTableInternal() {
		assertEquals(testTable, catalogManager.getRootSchema().getTable(TEST_TABLE));
	}

	@Test(expected = TableException.class)
	public void testRegisterTableInternalWithAlreadyExistTable() {
		catalogManager.registerTable(TEST_TABLE, testTable);
	}

	@Test
	public void testReplaceRegisteredTable() {
		AbstractTable replace = new RelTable(null);

		catalogManager.replaceRegisteredTable(TEST_TABLE, replace);

		Optional<Table> result = catalogManager.getTable(TEST_TABLE);
		assertTrue(result.isPresent());

		Table newTable = result.get();

		assertNotNull(newTable);
		assertNotEquals(testTable, newTable);
		assertEquals(replace, newTable);
	}

	@Test(expected = TableException.class)
	public void testReplaceNotExistTable() {
		catalogManager.replaceRegisteredTable("missing", null);
	}

	@Test
	public void testIsRegistered() {
		assertTrue(catalogManager.isRegistered(TEST_TABLE));
		assertFalse(catalogManager.isRegistered("missing"));
	}
}
