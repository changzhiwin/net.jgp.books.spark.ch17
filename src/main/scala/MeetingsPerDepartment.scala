package net.jgp.books.spark.ch17.lab210_analytics_dept

import org.apache.spark.sql.functions.col

import net.jgp.books.spark.basic.Basic

object MeetingsPerDepartment extends Basic {
  def run(): Unit = {
    val spark = getSession("Counting the number of meetings per department")

    val df = spark.read.format("delta").
      load("/tmp/ch17/delta_grand_debat_events").
      groupBy(col("authorDept")).
      count.
      orderBy(col("count").desc_nulls_first)

    df.show(10, true)

    spark.close
  }
}