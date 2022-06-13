package net.jgp.books.spark.ch17.lab210_analytics_type

import org.apache.spark.sql.functions.col

import net.jgp.books.spark.basic.Basic

object MeetingsPerType extends Basic {
  def run(): Unit = {
    val spark = getSession("Counting the number of meetings per department")

    val df = spark.read.format("delta").
      load("/tmp/ch17/delta_grand_debat_events").
      groupBy(col("authorType")).
      count.
      orderBy(col("count").desc_nulls_last)

    df.show(10, true)

    spark.close
  }
}