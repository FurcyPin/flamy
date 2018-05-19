package com.flaminem.flamy.exec.actions

import java.io.{FileOutputStream, PrintStream}

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.exec.utils.Action
import com.flaminem.flamy.model.TableInfo
import com.flaminem.flamy.model.metadata.{TableWithInfo, TableWithParams}
import com.flaminem.flamy.utils.AutoClose
import org.apache.hadoop.fs.{FileSystem, Path}

class PullTableAction(val tableInfo: TableInfo, tableParams: TableWithParams, schemaFolder: Path, context: FlamyContext) extends Action {

  override  val name: String = s"PULL TABLE ${tableInfo.fullName}"

  override  val logPath: String = s"PULL_TABLE_${tableInfo.fullName}"

  override def run(): Unit = {
    if(!context.dryRun){
      val destinationFolder: Path = schemaFolder.suffix("/" + tableInfo.tableName.name)
      val fs: FileSystem = context.getLocalFileSystem.fileSystem
      if(!fs.exists(destinationFolder)){
        fs.mkdirs(destinationFolder)
      }
      val destinationFile: String = destinationFolder.suffix("/" + "CREATE.hql").toString
      for {
        fos <- AutoClose(new FileOutputStream(destinationFile))
        ps <- AutoClose(new PrintStream(fos))
      } {
        tableInfo.isView
        val commentString =
          if(tableParams.comment.isDefined) {
            s" COMMENT '${tableParams.comment.get}'"
          }
          else {
            ""
          }
        val externalString =
          if(tableParams.isExternal){
            " EXTERNAL"
          }
          else {
            ""
          }
        val createString = s"CREATE$externalString TABLE ${tableParams.tableName}"
        val columnsString =
          tableInfo.columns.map{
            c => s"  ${c.formattedColumnName} ${c.columnType.get}"
          }.mkString("(\n", ",\n", "\n)\n")
        val partitionString =
          if(tableInfo.partitions.isEmpty){
            ""
          }
          else {
            tableInfo.partitions.map{
              p => s"  ${p.formattedColumnName} ${p.columnType.get}"
            }.mkString("PARTITIONED BY (\n", ",\n", "\n)\n")
          }
        val storedAsString = s"STORED AS ${tableParams.ioFormat.toString}\n"
        val locationString = s"LOCATION '${tableParams.location}'\n"
        ps.println(s"$createString$columnsString$partitionString$storedAsString$locationString;")
      }
    }
  }


}
