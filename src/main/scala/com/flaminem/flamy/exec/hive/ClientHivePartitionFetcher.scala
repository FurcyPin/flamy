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

import java.util
import javax.security.auth.Subject

import com.flaminem.flamy.conf.{FlamyConfVars, FlamyContext}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo, TableWithParams}
import com.flaminem.flamy.model.names.{SchemaName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{Database, MetaException, NoSuchObjectException}
import org.apache.hadoop.security.UserGroupInformation
import javax.security.auth.kerberos.KerberosPrincipal
import javax.security.auth.kerberos.KerberosTicket

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * Created by fpin on 11/3/14.
 */
class ClientHivePartitionFetcher(context: FlamyContext) extends HivePartitionFetcher {

  val hiveConf: HiveConf = new HiveConf(classOf[HiveMetaStoreClient])
  val hiveMetastoreUri: String = context.HIVE_METASTORE_URI.getProperty
  hiveConf.set("hive.metastore.uris", hiveMetastoreUri)
  
  val cli: IMetaStoreClient = new HiveMetaStoreClient(hiveConf)

  override def listSchemaNames: List[SchemaName] = {
    try {
      cli.getAllDatabases.toList.map {SchemaName(_)}
    }
    catch {
      case e: MetaException =>
        throw new FlamyException("Could not connect to the metastore via thrift. This generally happens when the metastore " +
          s"requires Kerberos authentication and it has not been correctly configured in flamy.")
    }
  }

  override def listSchemasWithInfo: Iterable[SchemaWithInfo] = {
    FlamyOutput.err.warn(
      "The ClientHivePartitionFetcher does not fetch the size, num_files nor modification_time because of performance issues. " +
      s"Consider trying the direct implementation by setting ${context.HIVE_META_FETCHER_TYPE.propertyKey} = direct"
    )
    cli.getAllDatabases.toList.map{ name =>
      val schema: Database = cli.getDatabase(name)
      val tables: util.List[String] = cli.getAllTables(name)
      new SchemaWithInfo(
        creationTime = None,
        location = schema.getLocationUri,
        name = SchemaName(schema.getName),
        comment = Option(schema.getDescription),
        numTables = Some(tables.size()),
        fileSize = None,
        fileCount = None,
        modificationTime = None
      )
    }
  }

  override def listSchemasWithLocation: Iterable[SchemaWithInfo] = {
    cli.getAllDatabases.toList.map{ name =>
      val schema: Database = cli.getDatabase(name)
      new SchemaWithInfo(
        creationTime = None,
        location = schema.getLocationUri,
        name = SchemaName(schema.getName),
        comment = Option(schema.getDescription),
        numTables = None,
        fileSize = None,
        fileCount = None,
        modificationTime = None
      )
    }
  }

  override def getTableWithInfo(tableName: TableName): Option[TableWithInfo] = {
    Option(cli.getTable(tableName.schemaName.name, tableName.name)).map{
      case t if t.getPartitionKeys.isEmpty => TableWithInfo(t)
      case t => TableWithInfo(t, getTablePartitioningInfo(tableName))
    }
  }

  override def getTableWithParams(tableName: TableName): Option[TableWithParams] = {
    Option(cli.getTable(tableName.schemaName.name, tableName.name)).map{
      table =>
        val sd = table.getSd
        val ioFormat = IOFormat(sd.getInputFormat, sd.getOutputFormat, sd.getSerdeInfo.getSerializationLib)
        new TableWithParams(
          tableName = TableName(table.getDbName, table.getTableName),
          location = sd.getLocation,
          ioFormat = ioFormat,
          params = table.getParameters.toMap
        )
    }
  }

  override def listTablesNamesInSchema(schema: SchemaName): List[TableName] = {
    cli.getAllTables(schema.fullName).toList.map{TableName(schema,_)}
  }

  override def getTable(t: TableName): Option[TableInfo] = {
    Option(cli.getTable(t.schemaName, t.name)).map{TableInfo(_)}
  }

  override def listPartitionNames(table: TableName): List[TablePartitionName] = {
    cli.listPartitionNames(table.schemaName, table.name, 0.asInstanceOf[Short]).toList.map{TablePartitionName(table, _)}
  }

  def getPartitionKeys(t: TableName): Seq[PartitionKey] = {
    cli.getTable(t.schemaName, t.name).getPartitionKeys.map{fieldSchema => new PartitionKey(new Column(fieldSchema))}
  }

  override def getTablePartitioningInfo(t: TableName): TablePartitioningInfo = {
    val partitionKeys: Seq[PartitionKey] = getPartitionKeys(t)
    val tablePartitions: Seq[PartitionWithInfo] =
      if(partitionKeys.isEmpty) {
        val table = cli.getTable(t.schemaName,t.name)
        val p = PartitionWithEagerInfo(table)
        if(p.creationTime == p.modificationTime) {
          /* If the modificationTime is the same as the creationTime, it means that the table was never populated */
          Nil
        }
        else {
          p::Nil
        }
      }
      else {
        cli.
          listPartitions(t.schemaName, t.name, -1)
          .map{p => PartitionWithEagerInfo(p, partitionKeys)}
      }
    new TablePartitioningInfo(t, partitionKeys, tablePartitions) {
      override def refreshAllFileStatus(context: FlamyContext): TablePartitioningInfo = this
    }
  }

  override def getPartition(tp: TablePartitionName): Option[PartitionWithInfo] = {
    try {
      val partitionKeys: Seq[PartitionKey] = getPartitionKeys(tp.tableName)
      Option(cli.getPartition(tp.schemaName, tp.tableName.name, tp.partColNames.map{_.toString})) match {
        case None => None
        case Some(p) => Some(PartitionWithEagerInfo(p, partitionKeys))
      }
    }
    catch {
      case e: NoSuchObjectException => None
    }
  }

  override def close(): Unit = {
    try {
      cli.close()
    }
    catch {
      case _ : java.lang.NoSuchMethodError =>
        /* If we get a NoSuchMethodError, we ignore it */
        ()
    }
  }

  override def clearCache(): Unit = {
    /* no cache used here */
  }

}

