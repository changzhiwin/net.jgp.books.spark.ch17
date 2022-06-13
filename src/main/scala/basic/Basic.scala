package net.jgp.books.spark.basic

import org.apache.spark.sql.SparkSession

trait Basic {
  def getSession(appName: String): SparkSession = {
    SparkSession.builder().
      appName(appName).
      config("spark.executor.memory", "4g").
      config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").
      config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").
      getOrCreate()
  }
}