/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.{Environment, FlamyConfVars, FlamyContext}
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo, TableWithParams}
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.logging.Logging

/**
 * Created by fpin on 2/5/15.
 */
trait HiveTableFetcher extends AutoCloseable{

  def listSchemaNames: Iterable[SchemaName]

  def listSchemasWithInfo: Iterable[SchemaWithInfo]

  def listSchemasWithLocation: Iterable[SchemaWithInfo]

  def listTableNamesInSchema(schema: SchemaName): Iterable[TableName]

  def getTable(tableName: TableName): Option[TableInfo]

  def getTableWithInfo(table: TableName): Option[TableWithInfo]

  def getTableWithParams(table: TableName): Option[TableWithParams]

  /* Since iterating over every table may be long, we handle interruptions */
  def listTablesWithInfo(itemFilter: ItemFilter): Iterable[TableWithInfo] = {
    listTableNames(itemFilter).flatMap{
      case _ if Thread.currentThread().isInterrupted => throw new InterruptedException
      case tableName => getTableWithInfo(tableName)
    }
  }

  /* Since iterating over every table may be long, we handle interruptions */
  def listTablesWithParams(itemFilter: ItemFilter): Iterable[TableWithParams] = {
    listTableNames(itemFilter).flatMap{
      case _ if Thread.currentThread().isInterrupted => throw new InterruptedException
      case tableName => getTableWithParams(tableName)
    }
  }

  def listTableNames: Iterable[TableName] = {
    listSchemaNames.flatMap{listTableNamesInSchema}
  }

  /**
    * List all the TableNames matching the specified ItemFilter
    * @param itemFilter
    * @return
    */
  def listTableNames(itemFilter: ItemFilter): Iterable[TableName] = {
    listTableNames.filter{itemFilter}
  }

  /**
    * List all the table names matching the specified ItemNames
    * @param items
    * @return
    */
  def listTableNames(items: ItemName*): Iterable[TableName] = {
    listTableNames(ItemFilter(items, acceptIfEmpty = true))
  }

  def listTables(itemFilter: ItemFilter): Iterable[TableInfo] = {
    listTableNames.filter{itemFilter}.flatMap{getTable}
  }

  def listTables(items: ItemName*): Iterable[TableInfo] = {
    if(items.isEmpty){
      listTableNames().flatMap{getTable}
    }
    else {
      val schemaNames = items.collect{case s : SchemaName => s}
      val tableNames: Seq[TableName] = (items.collect{case t : TableName => t} ++ schemaNames.flatMap(listTableNamesInSchema)).distinct.sorted
      tableNames.flatMap{getTable}
    }
  }

}

object HiveTableFetcher extends Logging{

  /**
   * Creates a new HiveMetaDataFetcher depending on the context.
   *
   * @param context
   * @return
   */
  def apply(context: FlamyContext): HiveTableFetcher = context.getEnvironment match {
    case Environment.MODEL_ENV => new ModelHiveTableFetcher(context)
    case _ => HivePartitionFetcher(context)
  }

}
