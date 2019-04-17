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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for GenericInMemoryCatalog.
 */
public class GenericInMemoryCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() {
		catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
		catalog.open();
	}

	@After
	public void cleanup() throws Exception {
		if (catalog.tableExists(path1)) {
			catalog.dropTable(path1, true);
		}
		if (catalog.tableExists(path2)) {
			catalog.dropTable(path2, true);
		}
		if (catalog.tableExists(path3)) {
			catalog.dropTable(path3, true);
		}
	}

	// ------ tables ------

	@Test
	public void testCreateTable_Streaming() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		GenericCatalogTable table = createStreamingTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testCreateTable_Batch() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogDatabase database = catalog.getDatabase(db1);
		assertTrue(TEST_COMMENT.equals(database.getDescription().get()));

		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogBaseTable tableCreated = catalog.getTable(path1);
		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) tableCreated);
		assertEquals(TABLE_COMMENT, tableCreated.getDescription().get());

		List<String> tables = catalog.listTables(db1);

		assertEquals(1, tables.size());
		assertEquals(path1.getObjectName(), tables.get(0));

		catalog.dropTable(path1, false);
	}

	@Test
	public void testCreateTable_DatabaseNotExistException() throws Exception {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.createTable(nonExistObjectPath, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1,  CatalogTestUtil.createTable(TABLE_COMMENT), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		GenericCatalogTable table =  CatalogTestUtil.createTable(TABLE_COMMENT);
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testGetTable_TableNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.nonexist does not exist in Catalog");
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testGetTable_TableNotExistException_NoDb() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.nonexist does not exist in Catalog");
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testDropTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testListTables() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path3, createTable(), false);
		catalog.createTable(path4, createView(), false);

		assertEquals(3, catalog.listTables(db1).size());
		assertEquals(1, catalog.listViews(db1).size());

		catalog.dropTable(path1, false);
		catalog.dropTable(path3, false);
		catalog.dropTable(path4, false);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropTable_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistObjectPath, true);
	}

	@Test
	public void testAlterTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		GenericCatalogTable table = CatalogTestUtil.createTable(TABLE_COMMENT);
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		GenericCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		CatalogTestUtil.checkEquals(newTable, (GenericCatalogTable) catalog.getTable(path1));

		catalog.dropTable(path1, false);
	}

	@Test
	public void testAlterTable_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterTable_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testRenameTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path3));
		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testRenameTable_TableNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.t1 does not exist in Catalog");
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testRenameTable_TableNotExistException_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(path1, t2, true);
	}

	@Test
	public void testRenameTable_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);
		catalog.createTable(path3, createAnotherTable(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t2 already exists in Catalog");
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testTableExists() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	// ------ views ------

	@Test
	public void testCreateView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testCreateView_DatabaseNotExistException() throws Exception {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.createTable(nonExistObjectPath, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createView(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
		catalog.createTable(path1, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherView(), true);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testDropView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createView(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testAlterView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));

		CatalogView newView = createAnotherView();
		catalog.alterTable(path1, newView, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(newView, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testAlterView_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterView_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createView(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testListView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listTables(db1).isEmpty());

		catalog.createTable(path1, createView(), false);
		catalog.createTable(path3, createTable(), false);

		assertEquals(2, catalog.listTables(db1).size());
		assertEquals(new HashSet<>(Arrays.asList(path1.getObjectName(), path3.getObjectName())),
			new HashSet<>(catalog.listTables(db1)));
		assertEquals(Arrays.asList(path1.getObjectName()), catalog.listViews(db1));
	}

	@Test
	public void testRenameView() throws Exception {
		catalog.createDatabase("db1", new GenericCatalogDatabase(new HashMap<>()), false);
		GenericCatalogView view = new GenericCatalogView("select * from t1",
			"select * from db1.t1", createTableSchema(), new HashMap<>());
		ObjectPath viewPath1 = new ObjectPath(db1, "view1");
		catalog.createTable(viewPath1, view, false);
		assertTrue(catalog.tableExists(viewPath1));
		catalog.renameTable(viewPath1, "view2", false);
		assertFalse(catalog.tableExists(viewPath1));
		ObjectPath viewPath2 = new ObjectPath(db1, "view2");
		assertTrue(catalog.tableExists(viewPath2));
		catalog.dropTable(viewPath2, false);
	}

	// ------ utilities ------

	@Override
	public String getBuiltInDefaultDatabase() {
		return GenericInMemoryCatalog.DEFAULT_DB;
	}

	@Override
	public CatalogDatabase createDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
		}}, TEST_COMMENT);
	}

	@Override
	public CatalogDatabase createAnotherDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
		}}, "this is another database.");
	}

	private GenericCatalogTable createStreamingTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getStreamingTableProperties(), TABLE_COMMENT);
	}

	@Override
	public GenericCatalogTable createTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getBatchTableProperties(), TABLE_COMMENT);
	}

	private GenericCatalogTable createAnotherTable() {
		return CatalogTestUtil.createTable(
			createAnotherTableSchema(),
			getBatchTableProperties(), TABLE_COMMENT);
	}

	private Map<String, String> getBatchTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "false");
		}};
	}

	private Map<String, String> getStreamingTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "true");
		}};
	}

	private TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
			}
		);
	}

	private TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			}
		);
	}

	private CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	private CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

}
