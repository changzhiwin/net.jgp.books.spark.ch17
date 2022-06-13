package net.jgp.books.spark.ch17.lab200_feed_delta

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{ col, when, expr }
import org.apache.spark.sql.types._

import net.jgp.books.spark.basic.Basic

// bin/spark-shell --packages io.delta:delta-core_2.13:1.2.1 
//   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
//   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

object FeedDeltaLake extends Basic {

  val log = Logger(getClass.getName)

  def run(): Unit = {
    val spark = getSession("Ingestion the files to Delta Lake")

    val df = spark.read.format("json").
      schema(
        StructType(
          StructField("id", StringType, true) ::
          StructField("title", StringType, true) ::
          StructField("createdAt", TimestampType, true) ::
          StructField("updatedAt", TimestampType, true) ::
          StructField("startAt", TimestampType, true) ::
          StructField("endAt", TimestampType, true) ::
          StructField("enabled", BooleanType, true) ::
          StructField("lat", DoubleType, true) ::
          StructField("lng", DoubleType, true) ::
          StructField("fullAddress", StringType, true) ::
          StructField("link", StringType, true) ::
          StructField("url", StringType, true) ::
          StructField("body", StringType, true) ::
          StructField("authorId", StringType, true) ::
          StructField("authorType", StringType, true) ::
          StructField("authorZipCode", StringType, true) :: Nil
        )
      ).
      option("timestampFormat", "yyyy-MM-dd HH:mm:ss").
      load("data/france_grand_debat/20190302 EVENTS.json").
      // with transfrom
      withColumn("authorZipCode", col("authorZipCode").cast(IntegerType)).
      withColumn("authorZipCode", 
        when(col("authorZipCode") < 1000, null).
        when(col("authorZipCode") > 99999, null).
        otherwise(col("authorZipCode"))
      ).
      withColumn("authorDept", expr("int(authorZipCode / 1000)"))
    
    df.printSchema
    df.sample(0.01).show(20)

    df.write.format("delta").
      mode("overwrite").
      save("/tmp/ch17/delta_grand_debat_events")

    log.info("### {} rows updated", df.count)
  }
}