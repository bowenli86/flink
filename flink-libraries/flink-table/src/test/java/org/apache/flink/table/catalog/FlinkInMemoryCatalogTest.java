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

import org.junit.Test;

/**
 * Test for FlinkInMemoryCatalog.
 */
public class FlinkInMemoryCatalogTest {
	private static final String SUB_CATALOG_NAME = "t";

	private FlinkInMemoryCatalog catalog = new FlinkInMemoryCatalog("test");

	@Test (expected = UnsupportedOperationException.class)
	public void testCreateSubCatalog() {
		catalog.createSubCatalog(SUB_CATALOG_NAME, null, true);
	}

	@Test (expected = UnsupportedOperationException.class)
	public void testDropSubCatalog() {
		catalog.dropSubCatalog(SUB_CATALOG_NAME, true);
	}

	@Test (expected = UnsupportedOperationException.class)
	public void testAlterSubCatalog() {
		catalog.alterSubCatalog(SUB_CATALOG_NAME, null, true);
	}

	@Test (expected = UnsupportedOperationException.class)
	public void testGetSubCatalog() {
		catalog.getSubCatalog(SUB_CATALOG_NAME);
	}

	@Test (expected = UnsupportedOperationException.class)
	public void testListSubCatalogs() {
		catalog.listSubCatalogs();
	}
}
