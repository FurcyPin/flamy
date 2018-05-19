package com.flaminem.flamy.exec.actions

import java.io.{FileOutputStream, PrintStream}

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.exec.utils.Action
import com.flaminem.flamy.model.metadata.SchemaWithInfo
import com.flaminem.flamy.utils.AutoClose
import org.apache.hadoop.fs.{FileSystem, Path}

class PullSchemaAction(val schemaWithInfo: SchemaWithInfo, context: FlamyContext) extends Action {

  override  val name: String = s"PULL SCHEMA ${schemaWithInfo.name}"

  override  val logPath: String = s"PULL_SCHEMA_${schemaWithInfo.name}"

  override def run(): Unit = {
    if(!context.dryRun){
      val schemaFolder: String = context.modelDirs.head.getAbsolutePath + "/pull/" + schemaWithInfo.name + ".db"
      val fs: FileSystem = context.getLocalFileSystem.fileSystem
      fs.mkdirs(new Path(schemaFolder))
      val destinationFile: String = schemaFolder + "/" + "CREATE_SCHEMA.hql"
      for {
        fos <- AutoClose(new FileOutputStream(destinationFile))
        ps <- AutoClose(new PrintStream(fos))
      } {
        val commentString =
          if(schemaWithInfo.comment.isDefined) {
            s" COMMENT '${schemaWithInfo.comment.get}'"
          }
          else {
            ""
          }
        val locationString = s" LOCATION '${schemaWithInfo.location}'"
        ps.println(s"CREATE SCHEMA ${schemaWithInfo.name}$commentString$locationString ;")
      }
    }
  }


}
