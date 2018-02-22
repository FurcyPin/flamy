package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.FlamyContext
import org.scalatest.FreeSpec

class ModelHiveTableFetcherTest extends FreeSpec {

  val context: FlamyContext =
    new FlamyContext(
      "flamy.model.dir.paths" -> "src/test/resources/ModelHiveTableFetcher",
      "flamy.variables.path" -> "src/test/resources/ModelHiveTableFetcher/VARIABLES.properties"
    )
  val fetcher = new ModelHiveTableFetcher(context)

  "listTables should work" in {
    assert(fetcher.listTables().size == 8)
    assert(fetcher.listTables("db_dest.dest", "db_dest.dest1", "db_dest.dest2").size == 3)
    assert(fetcher.listTables("db_dest").size == 5)
  }



}
