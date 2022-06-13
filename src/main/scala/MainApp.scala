package net.jgp.books.spark

import  net.jgp.books.spark.ch17.lab100_export.ExportWildfires
import net.jgp.books.spark.ch17.lab200_feed_delta.FeedDeltaLake
import net.jgp.books.spark.ch17.lab210_analytics_dept.MeetingsPerDepartment
import net.jgp.books.spark.ch17.lab210_analytics_type.MeetingsPerType
import net.jgp.books.spark.ch17.lab21x_crud.EventsCURD

//MeetingsPerType

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("DEPT", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "CRUD"     => EventsCURD.run()
      case "TYPE"     => MeetingsPerType.run()
      case "DEPT"     => MeetingsPerDepartment.run()
      case "DELTA"    => FeedDeltaLake.run()
      case _          => ExportWildfires.run()
    }
  }
}