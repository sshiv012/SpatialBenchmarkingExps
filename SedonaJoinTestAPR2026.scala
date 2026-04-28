import java.io.{BufferedWriter, File, FileWriter}
  import java.time.ZonedDateTime
  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.execution.SparkPlan
  import org.apache.sedona.sql.utils.SedonaSQLRegistrator

  SedonaSQLRegistrator.registerAll(spark)

  val logPath = "query_log_show_collect_apr_17_26.txt"

  def appendLog(s: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(new File(logPath), true))
    try bw.write(s) finally bw.close()
  }

  def ts: String = ZonedDateTime.now().toString
  def gib(bytes: Long): Double = bytes.toDouble / (1024d * 1024d * 1024d)

  def logSparkRuntime(): Unit = {
    val conf = spark.sparkContext.getConf
    val keys = Seq(
      "spark.app.id",
      "spark.master",
      "spark.dynamicAllocation.enabled",
      "spark.executor.instances",
      "spark.executor.cores",
      "spark.executor.memory",
      "spark.driver.memory",
      "spark.sql.shuffle.partitions",
      "spark.sql.adaptive.enabled"
    )

    appendLog("\n==== Spark Runtime ====\n")
    appendLog(s"timestamp=$ts\n")
    appendLog(s"ui=${spark.sparkContext.uiWebUrl.getOrElse("n/a")}\n")
    keys.foreach { k =>
      appendLog(s"$k=${conf.getOption(k).getOrElse("undefined")}\n")
    }
  }

  def logDriverMemory(label: String): Unit = {
    val rt = Runtime.getRuntime
    val maxMem = rt.maxMemory()
    val totalMem = rt.totalMemory()
    val freeMem = rt.freeMemory()
    val usedMem = totalMem - freeMem

    appendLog(s"\n---- DRIVER MEMORY: $label ----\n")
    appendLog(f"maxGiB=${gib(maxMem)}%.2f totalGiB=${gib(totalMem)}%.2f usedGiB=${gib(usedMem)}%.2f freeGiB=${gib(freeMem)}%.2f\n")
  }

  def logExecutorStorageMemory(label: String): Unit = {
    appendLog(s"\n---- EXECUTOR STORAGE MEMORY: $label ----\n")
    spark.sparkContext.getExecutorMemoryStatus.toSeq.sortBy(_._1).foreach {
      case (exec, (maxMem, remaining)) =>
        val used = maxMem - remaining
        appendLog(
          f"$exec maxGiB=${gib(maxMem)}%.2f usedGiB=${gib(used)}%.2f freeGiB=${gib(remaining)}%.2f\n"
        )
    }
  }

  def logPlan(df: DataFrame, label: String): Unit = {
    appendLog(s"\n---- PLAN: $label ----\n")
    appendLog(df.queryExecution.toString + "\n")
    appendLog("---- EXECUTED PLAN ----\n")
    appendLog(df.queryExecution.executedPlan.toString + "\n")
  }

  def logPlanMetrics(df: DataFrame, label: String): Unit = {
    def walk(node: SparkPlan, depth: Int): Unit = {
      val indent = "  " * depth
      val metrics = node.metrics.toSeq.sortBy(_._1).map {
        case (k, m) => s"$k=${m.value}"
      }.mkString(", ")
      appendLog(s"$indent${node.nodeName}${if (metrics.nonEmpty) s" [$metrics]" else ""}\n")
      node.children.foreach(ch => walk(ch, depth + 1))
    }

    appendLog(s"\n---- PLAN METRICS: $label ----\n")
    walk(df.queryExecution.executedPlan, 0)
  }

  def runShowAndCollect(query: String, description: String): Unit = {
    val header =
      s"""
         |==== $description ====
         |timestamp=$ts
         |SQL: $query
         |""".stripMargin

    println(header)
    appendLog(header)

    val df = spark.sql(query)

    logPlan(df, s"$description / before-actions")
    logDriverMemory(s"$description / before-actions")
    logExecutorStorageMemory(s"$description / before-actions")

    val sc = spark.sparkContext

    val showGroup = s"show-${System.currentTimeMillis()}"
    sc.setJobGroup(showGroup, s"$description [show]")
    val tShow0 = System.nanoTime()
    df.show(truncate = false)
    val tShow1 = System.nanoTime()
    val showSec = (tShow1 - tShow0) / 1e9d
    val showJobIds = sc.statusTracker.getJobIdsForGroup(showGroup).sorted.mkString(",")

    logDriverMemory(s"$description / after-show")
    logExecutorStorageMemory(s"$description / after-show")

    val collectGroup = s"collect-${System.currentTimeMillis()}"
    sc.setJobGroup(collectGroup, s"$description [collect]")
    val tCollect0 = System.nanoTime()
    val rows = df.collect()
    val tCollect1 = System.nanoTime()
    val collectSec = (tCollect1 - tCollect0) / 1e9d
    val collectJobIds = sc.statusTracker.getJobIdsForGroup(collectGroup).sorted.mkString(",")

    appendLog("---- COLLECT RESULT ----\n")
    appendLog(rows.mkString("\n"))
    appendLog("\n")

    val totalActionSec = showSec + collectSec
    val summary =
      f"show_sec=$showSec%.6f collect_sec=$collectSec%.6f total_action_sec=$totalActionSec%.6f rows=${rows.length} show_jobIds=$showJobIds collect_jobIds=$collectJobIds\n"

    println(summary.trim)
    appendLog(summary)

    logDriverMemory(s"$description / after-collect")
    logExecutorStorageMemory(s"$description / after-collect")
    logPlanMetrics(df, s"$description / after-collect")
  }

  spark.catalog.clearCache()
  logSparkRuntime()

  println("Preparing buildings view...")
  val buildingsDF = spark.read.json("hdfs:///user/asevi006/osm2105_buildings_adm/")
  buildingsDF.createOrReplaceTempView("buildings_raw")

  val spatialBuildingsDF =
    spark.sql("SELECT ST_GeomFromWKT(g) AS g, `$1`, `$2` FROM buildings_raw")
  spatialBuildingsDF.createOrReplaceTempView("buildings_dataset")

  val filteredBuildingsDF =
    spark.sql("SELECT * FROM buildings_dataset WHERE ST_GeometryType(g) = 'ST_Polygon'")
  filteredBuildingsDF.createOrReplaceTempView("buildings_dataset")

  println("Preparing points view...")
  val pointsDF = spark.read.json("hdfs:///user/asevi006/osm2105_all_nodes_adm/")
  pointsDF.createOrReplaceTempView("all_nodes_raw")

  val spatialPointsDF =
    spark.sql("SELECT ST_GeomFromWKT(g) AS g, `attr#0`, `attr#1` FROM all_nodes_raw")
  spatialPointsDF.createOrReplaceTempView("all_nodes_dataset")

  val spatialJoinQuery =
    """SELECT COUNT(*) AS c
      |FROM all_nodes_dataset a, buildings_dataset b
      |WHERE ST_Contains(b.g, a.g)""".stripMargin

  runShowAndCollect(
    spatialJoinQuery,
    "Spatial Join: Count of points contained in buildings (show+collect)"
  )

  println("\nPress ENTER to exit the Spark console...")
  scala.io.StdIn.readLine()

  spark.stop()

