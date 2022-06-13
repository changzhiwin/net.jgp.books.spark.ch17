package net.jgp.books.spark.ch17.lab21x_crud

import io.delta.tables._
import org.apache.spark.sql.functions._

import net.jgp.books.spark.basic.Basic

object EventsCURD extends Basic {
  def run(): Unit = {

    val spark = getSession("Try delta crud")

    val deltaTable = DeltaTable.forPath(spark, "/tmp/ch17/delta_grand_debat_events")

    deltaTable.delete(condition = expr("authorDept > 14"))

    deltaTable.update(
      condition = expr("authorDept == 12"),
      set = Map("title" -> lit("This example runs a batch job to overwrite the data in the table"))
    )

    deltaTable.toDF.sample(0.01).show(true)
  }
}