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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.names.TableName
import org.scalatest.FreeSpec

class ModelHiveTableFetcherTest extends FreeSpec {

  val context: FlamyContext =
    new FlamyContext(
      "flamy.model.dir.paths" -> "src/test/resources/ModelHiveTableFetcher",
      "flamy.variables.path" -> "src/test/resources/ModelHiveTableFetcher/VARIABLES.properties"
    )
  val fetcher = new ModelHiveTableFetcher(context)

  "listTables(items: ItemName*) should work" in {
    assert(fetcher.listTables().size == 8)
    assert(fetcher.listTables("db_dest.dest", "db_dest.dest1", "db_dest.dest2").size == 3)
    assert(fetcher.listTables("db_dest").size == 5)
    assert(fetcher.listTables("db_dest", "db_dest.dest", "db_dest.dest1", "db_dest.dest2").size == 5)
    assert(fetcher.listTables("db_source").map{_.tableName}.toSet === Set(TableName("db_source","source"), TableName("db_source","source_view")) )
    assert(fetcher.listTables("db_source", "db_dest.dest").map{_.tableName}.toSet === Set(TableName("db_source","source"), TableName("db_source","source_view"), TableName("db_dest","dest")) )
    assert(fetcher.listTables("db_dest.dest", "db_dest.dest1").map{_.tableName}.toSet === Set(TableName("db_dest","dest"), TableName("db_dest","dest1")) )
  }


}
