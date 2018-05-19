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

package com.flaminem.flamy.model

import org.apache.hadoop.hive.metastore.api.StorageDescriptor

/**
 * Hive storage format of the table.
 */
case class IOFormat (
  inputFormat: String,
  outputFormat: String,
  serde: String
) {

  /**
    * Recognize common formats and return their generic names.
    * For other formats, the full information is printed.
    * @return
    */
  override def toString: String = toStandardString._1

    /**
    * Recognize common formats and return their generic names.
    * For other formats, the full information is printed.
    * @return A pair indicating the standard string, and if this correspond to a built-in Hive type.
    */
  def toStandardString: (String, Boolean) = this match {
    case
      IOFormat(
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
      )
      => ("ORC", true)
    case
      IOFormat(
        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
        "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
        "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
      )
      => ("RCFILE", true)
    case
      IOFormat(
        "org.apache.hadoop.mapred.SequenceFileInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      )
      => ("SEQUENCEFILE", true)
    case
      IOFormat(
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
        "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
      )
      => ("AVRO", true)
    case
      IOFormat(
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      )
      => ("TEXTFILE", true)
    case
      IOFormat(
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      )
      => ("PARQUET", true)
    case
      IOFormat(
        "parquet.hive.DeprecatedParquetInputFormat",
        "parquet.hive.DeprecatedParquetOutputFormat",
        "parquet.hive.serde.ParquetHiveSerDe"
      )
      => ("PARQUET (deprecated)", false)
    case
      IOFormat(
      "com.backtype.hadoop.pail.SequenceFileFormat$SequenceFilePailInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      _
      )
    => ("PAIL", false)
    case
      IOFormat(
        "org.apache.hadoop.mapred.SequenceFileInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        null
      )
      => ("VIEW", false) /* this table is a view */
    case
      IOFormat(
        null,
        null,
        null
      )
    => ("PRESTO VIEW", false) /* views created by presto have no IOFormat at all */
    case
      IOFormat(
        null,
        null,
        "org.apache.hadoop.hive.hbase.HBaseSerDe"
      )
      => ("HBASE", false) // this table is a view
    case _ => (s"TableFormat($inputFormat,$outputFormat,$serde)", false)
  }

}

object IOFormat {

  def apply(sd: StorageDescriptor): IOFormat = {
    IOFormat(sd.getInputFormat, sd.getOutputFormat, sd.getSerdeInfo.getSerializationLib)
  }


}