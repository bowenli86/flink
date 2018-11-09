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

package org.apache.flink.table.catalog

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util

import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.schema.SchemaPlus
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test
import collection.JavaConversions._


class ExternalCatalogHierachyTest extends TableTestBase {

  private var externalCatalogSchema: SchemaPlus = _
  private var calciteCatalogReader: CalciteCatalogReader = _

  @Test
  def testHierarchy(): Unit = {
    val javaEnv = new LocalStreamEnvironment()
    javaEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val env = new StreamExecutionEnvironment(javaEnv)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val catalog1 = getInMemoryTestCatalog(isStreaming = true)
    val catalog2 = getInMemoryTestCatalog2(isStreaming = true)


    // Register the 1st catalog
    tEnv.registerExternalCatalog("cat1", catalog1)

    printSchemaTree(tEnv.rootSchema, "")
    /*

    schema:   tables: []
        schema: cat1  tables: []
            schema: db  tables: [tb1-1]

    */


    System.out.println("--------------")


    // Register the 2nd catalog
    tEnv.registerExternalCatalog("cat2", catalog2)

    printSchemaTree(tEnv.rootSchema, "")
    /*

    schema:   tables: []
        schema: cat1  tables: []
            schema: db  tables: [tb1-1]
        schema: cat2  tables: []
            schema: db  tables: [tb2-2]

    * */

    System.out.println(tEnv.sqlQuery("select * from db.tb1"))
    System.out.println(tEnv.sqlQuery("select * from cat2.db.tb2"))
  }

  def printSchemaTree(schema: SchemaPlus, indent: String): Unit = {
    var newlist = new util.ArrayList[SchemaPlus]()

    System.out.println(indent + "schema: " + schema.getName + "  tables: " + schema.getTableNames)

    for (sub: String <- schema.getSubSchemaNames) {
      var subschema = schema.getSubSchema(sub)

      printSchemaTree(subschema, indent + "    ")
    }
  }

  def getInMemoryTestCatalog(isStreaming: Boolean): ExternalCatalog = {
    val csvRecord1 = Seq(
      "1#1#Hi"
    )
    val tempFilePath1 = writeToTempFile(csvRecord1.mkString("\n"), "csv-test1", "tmp")

    val connDesc1 = FileSystem().path(tempFilePath1)
    val formatDesc1 = Csv()
      .field("a", Types.INT)
      .fieldDelimiter("#")
    val schemaDesc1 = Schema()
      .field("a", Types.INT)
    val externalTableBuilder1 = ExternalCatalogTable.builder(connDesc1)
      .withFormat(formatDesc1)
      .withSchema(schemaDesc1)

    if (isStreaming) {
      externalTableBuilder1.inAppendMode()
    }

    val catalog = new InMemoryExternalCatalog("cat-----1")
    val db1 = new InMemoryExternalCatalog("db")
    catalog.createSubCatalog("db", db1, ignoreIfExists = false)

    // Register the table with both catalogs
    db1.createTable("tb1", externalTableBuilder1.asTableSource(), ignoreIfExists = false)
    catalog
  }

  def getInMemoryTestCatalog2(isStreaming: Boolean): ExternalCatalog = {
    val csvRecord1 = Seq(
      "1#1#Hi"
    )
    val tempFilePath1 = writeToTempFile(csvRecord1.mkString("\n"), "csv-test1", "tmp")

    val connDesc1 = FileSystem().path(tempFilePath1)
    val formatDesc1 = Csv()
      .field("a1", Types.INT)
      .fieldDelimiter("#")
    val schemaDesc1 = Schema()
      .field("a1", Types.INT)
    val externalTableBuilder1 = ExternalCatalogTable.builder(connDesc1)
      .withFormat(formatDesc1)
      .withSchema(schemaDesc1)

    if (isStreaming) {
      externalTableBuilder1.inAppendMode()
    }

    val catalog = new InMemoryExternalCatalog("cat----2")
    val db1 = new InMemoryExternalCatalog("db")
    catalog.createSubCatalog("db", db1, ignoreIfExists = false)

    // Register the table with both catalogs
    db1.createTable("tb2", externalTableBuilder1.asTableSource(), ignoreIfExists = false)
    catalog
  }

  private def writeToTempFile(
    contents: String,
    filePrefix: String,
    fileSuffix: String,
    charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}
