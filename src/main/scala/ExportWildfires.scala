package net.jgp.books.spark.ch17.lab100_export

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.{ lit, expr, col, from_unixtime, unix_timestamp, when, isnull }

import net.jgp.books.spark.basic.Basic

object ExportWildfires extends Basic {

  val log = Logger(getClass.getName)

  def run(): Unit = {
    var spark = getSession("Wildfire data pipeline")

    val viirsMidDF = loadCsvDataFrame(spark, "data/wildfires/VNP14IMGTDL_NRT_Global_24h.csv")
    val viirsDF    = processViirsDF(viirsMidDF)
    showDF(viirsDF)

    val moidsMidDF = loadCsvDataFrame(spark, "data/wildfires/MODIS_C6_Global_24h.csv")
    val moidsDF    = processModisDF(moidsMidDF)
    showDF(moidsDF)

    
    val unionDF = viirsDF.unionByName(moidsDF)
    showDF(unionDF)
    log.info("### of partitions: {}", unionDF.rdd.getNumPartitions)

    unionDF.write.format("parquet").
      mode("overwrite").
      save("/tmp/ch17/fires_parquet")
    
    val outputDF = unionDF.filter(col("confidence_level") === "high").
      repartition(1).
      write.format("csv").
      option("header", true).
      mode("overwrite").
      save("/tmp/ch17/high_confidence_fires_csv")

    spark.close
  }

  private def processViirsDF(df: DataFrame): DataFrame = {
    df.withColumnRenamed("confidence", "confidence_level").
      withColumn("brightness", lit(null)).
      withColumn("bright_t31", lit(null))
  }

  private def processModisDF(df: DataFrame): DataFrame = {
    val low = 40
    val high = 100
    df.withColumn("bright_ti4", lit(null)).
      withColumn("bright_ti5", lit(null)).
      withColumn("confidence_level",
        when(col("confidence") <= low, "low").
        when(col("confidence") < high, "nominal").
        otherwise("high")
      ).
      drop("confidence")

    // df.select(col("confidence"), col("confidence_level")).filter(isnull(col("confidence_level"))).show(10, true)
  }

  private def loadCsvDataFrame(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      load(path).
      // with common transfrom
      withColumn("acq_time_hr", expr("int(acq_time / 100)")).
      withColumn("acq_time_min", expr("acq_time % 100")).
      withColumn("acq_time2", unix_timestamp(col("acq_date"), "yyyy-MM-dd")).
      withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")).
      withColumn("acq_datetime", from_unixtime(col("acq_time3"), "yyyy-MM-dd HH:mm:ss")).
      drop("acq_time3").
      drop("acq_time2").
      drop("acq_time_min").
      drop("acq_time_hr").
      drop("acq_time").
      drop("acq_date")
  }

  private def showDF(df: DataFrame): Unit = {
    df.printSchema
    df.sample(0.01).show(5)
  }
}