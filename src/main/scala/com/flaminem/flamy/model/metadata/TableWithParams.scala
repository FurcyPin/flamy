package com.flaminem.flamy.model.metadata

import com.flaminem.flamy.model.IOFormat
import com.flaminem.flamy.model.names.TableName

class TableWithParams(
  val tableName: TableName,
  val location: String,
  val ioFormat: IOFormat,
  val params: Map[String, String] = Map()
) {

  val comment: Option[String] = params.get("comment")

  val isExternal: Boolean = {
    params.get("EXTERNAL") match {
      case Some("TRUE") => true
      case _ => false
    }
  }

}
