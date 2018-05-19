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

package com.flaminem.flamy.commands

import com.flaminem.flamy.commands.utils.FlamySubcommand
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.actions.{PullSchemaAction, PullTableAction}
import com.flaminem.flamy.exec.hive.{HiveTableFetcher, ModelHiveTableFetcher}
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.files.{FileType, SchemaFile}
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo, TableWithParams}
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import org.apache.hadoop.fs.Path
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
  * Created by fpin on 5/22/15.
  */
class Pull extends Subcommand("pull") with FlamySubcommand {

  def pullSchemas(sourceContext: FlamyContext, destContext: FlamyContext, items: Seq[SchemaName]) = {
    val itemFilter = ItemFilter(items, acceptIfEmpty = true)
    val destFetcher = new ModelHiveTableFetcher(destContext, items: _*)
    val sourceFetcher = HiveTableFetcher(sourceContext)
    val sourceSchemas: Set[SchemaName] = sourceFetcher.listSchemaNames.filter{itemFilter}.toSet
    val destSchemas: Set[SchemaName] = destFetcher.listSchemaNames.filter{itemFilter}.toSet

    val missingSchemas: Set[SchemaName] = sourceSchemas.diff(destSchemas)
    val missingSchemasWithLocation: Iterable[SchemaWithInfo] = sourceFetcher.listSchemasWithLocation.filter{s => missingSchemas.contains(s.name)}

    val runner = new ActionRunner(silentOnSuccess = false, silentOnFailure = false)
    val pullSchemaActions = missingSchemasWithLocation.map{new PullSchemaAction(_, destContext)}

    runner.run(pullSchemaActions)
    ReturnStatus(success = runner.getStats.getFailCount == 0)
  }

  val schemas: FlamySubcommand = new Subcommand("schemas") with FlamySubcommand {
    banner("Create in the model the schemas that are present in the specified environment and missing in the model")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to pull from", required = true, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val items: ScallopOption[List[String]] =
      trailArg[List[String]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val sourceContext = new FlamyContext(globalOptions, environment.get)
      val destContext = new FlamyContext(globalOptions)
      sourceContext.dryRun = dryRun()
      destContext.dryRun = dryRun()
      pullSchemas(sourceContext, destContext, items().map{SchemaName(_)})
    }

  }

  val tables: FlamySubcommand = new Subcommand("tables") with FlamySubcommand {
    banner("Create in the model the tables that are present in the specified environment and missing in the model")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to pull from", required = false, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val sourceContext = new FlamyContext(globalOptions, environment.get)
      val destContext = new FlamyContext(globalOptions)
      sourceContext.dryRun = dryRun()
      destContext.dryRun = dryRun()

      val itemFilter = ItemFilter(items(), acceptIfEmpty = true)
      val destFetcher = new ModelHiveTableFetcher(destContext)
      val sourceFetcher = HiveTableFetcher(sourceContext)
      val sourceTables: Set[TableName] = sourceFetcher.listTableNames.filter{itemFilter}.toSet
      val destTables: Set[TableName] = destFetcher.listTableNames.filter{itemFilter}.toSet

      val missingTables: Set[TableName] = sourceTables.diff(destTables)
      val missingTablesFilter = ItemFilter(missingTables, acceptIfEmpty = false)
      val missingTableInfos: Seq[TableInfo] = sourceFetcher.listTables(missingTablesFilter).toSeq.sortBy(_.tableName)
      val missingTableParams: Seq[TableWithParams] = sourceFetcher.listTablesWithParams(missingTablesFilter).toSeq.sortBy(_.tableName)
      assert(missingTableInfos.size == missingTableParams.size)

      val schemaNames: Seq[SchemaName] = missingTables.map{_.schemaName}.toSeq.distinct

      val pullSchemaResult: ReturnStatus = pullSchemas(sourceContext, destContext, schemaNames)
      if(pullSchemaResult.isFailure) {
        pullSchemaResult
      }
      else {
        val schemaFiles: Iterable[SchemaFile] = destFetcher.fileIndex.getAllSchemaFilesOfType(FileType.CREATE_SCHEMA)
        val schemaFolders: Map[SchemaName, Path] = schemaFiles.map{schemaFile => schemaFile.schemaName -> schemaFile.path.getParent}.toMap

        val runner = new ActionRunner(silentOnSuccess = false, silentOnFailure = false)
        val pullTableActions =
          missingTableInfos.zip(missingTableParams).map{
            case (tableInfo, tableParams) =>
            assert(tableInfo.tableName == tableParams.tableName)
            val schemaFolder =
              schemaFolders.getOrElse(
                tableInfo.schemaName,
                new Path(destContext.modelDirs.head.getAbsolutePath + "/pull/" + tableInfo.schemaName.name + ".db")
              )
            new PullTableAction(tableInfo, tableParams, schemaFolder, destContext)
          }
        runner.run(pullTableActions)
        ReturnStatus(success = runner.getStats.getFailCount == 0)
      }

    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {

    subCommands match {
      case (command: FlamySubcommand) :: Nil =>
        command.doCommand(globalOptions, Nil)
      case _ => printHelp()
    }
    ReturnSuccess
  }

}
