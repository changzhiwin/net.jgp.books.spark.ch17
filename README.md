# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch17

# Environment
- Java 8
- Scala 2.13.8
- Spark 3.2.1
- https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_Global_24h.csv

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch17_2.13-1.0.jar` (the same as name property in sbt file)


## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit    \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --class net.jgp.books.spark.MainApp  \
  --master "local[*]"   \
  --packages io.delta:delta-core_2.13:1.2.1,com.typesafe.scala-logging:scala-logging_2.13:3.9.4 \
  target/scala-2.13/main-scala-ch17_2.13-1.0.jar
```

## 3, print

### Case: 
```
root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- bright_ti4: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_ti5: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- brightness: void (nullable = true)
 |-- bright_t31: void (nullable = true)

root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- brightness: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- confidence: integer (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_t31: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- bright_ti4: void (nullable = true)
 |-- bright_ti5: void (nullable = true)
 |-- confidence_level: string (nullable = false)

root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- bright_ti4: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_ti5: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- brightness: double (nullable = true)
 |-- bright_t31: double (nullable = true)

### of partitions: 2
```

## 4, Some diffcult case

### --jars vs --packages
```
--jars 

--packages io.delta:delta-core_2.13:1.2.1,com.typesafe.scala-logging:scala-logging_2.13:3.9.4 \
```


### unionByName will merge void type to double
```
// fill df 1, with(brightness void, bright_t31 double)
  df1.
    withColumn("brightness", lit(null))

// df2 with(brightness double, bright_t31 void)
  df2.
    withColumn("bright_t31", lit(null))

// result with(brightness double, bright_t31 double)
  df1.unionByName(df2)
```

### unix_timestamp & from_unixtime need format, otherwise get null

```
      withColumn("acq_time2", unix_timestamp(col("acq_date"))).     // , "yyyy-MM-dd"
      withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")).
      withColumn("acq_datetime", from_unixtime(col("acq_time3"))).  // , "yyyy-MM-dd HH:mm:ss"

// acq_datetime get null
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+------------+----------+----------+
|latitude|longitude|bright_ti4|scan|track|satellite|confidence_level|version|bright_ti5| frp|daynight|acq_datetime|brightness|bright_t31|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+------------+----------+----------+
|-2.29525| 31.12142|     299.0|0.53| 0.67|        N|         nominal| 1.0NRT|     283.6| 4.7|       N|        null|      null|      null|
|-5.71879| 27.78929|     367.0|0.35| 0.57|        N|            high| 1.0NRT|     289.0|10.0|       N|        null|      null|      null|
|-6.43091| 27.73394|     327.9|0.36| 0.57|        N|         nominal| 1.0NRT|     286.2| 2.7|       N|        null|      null|      null|
|-6.58869| 27.72601|     320.5|0.36| 0.58|        N|         nominal| 1.0NRT|     286.2| 4.7|       N|        null|      null|      null|
|-6.89026| 25.79821|     305.5|0.54| 0.51|        N|         nominal| 1.0NRT|     288.9|31.2|       N|        null|      null|      null|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+------------+----------+----------+
```
Then, setting the format paramter, it works.
```
      withColumn("acq_time2", unix_timestamp(col("acq_date"), "yyyy-MM-dd")).
      withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")).
      withColumn("acq_datetime", from_unixtime(col("acq_time3"), "yyyy-MM-dd HH:mm:ss")).

// acq_datetime works
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|latitude|longitude|bright_ti4|scan|track|satellite|confidence_level|version|bright_ti5| frp|daynight|       acq_datetime|brightness|bright_t31|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|12.82222| 13.91569|     367.0|0.36| 0.57|        N|            high| 1.0NRT|     294.6| 5.7|       N|2021-07-06 00:00:00|      null|      null|
|-2.13094| 31.11913|     301.7|0.53| 0.67|        N|         nominal| 1.0NRT|     284.8| 0.9|       N|2021-07-06 00:00:00|      null|      null|
|-2.48683| 31.19605|     297.4|0.54| 0.68|        N|         nominal| 1.0NRT|     284.8| 1.4|       N|2021-07-06 00:00:00|      null|      null|
|-5.22873|   24.978|     338.7|0.45| 0.47|        N|         nominal| 1.0NRT|     292.0| 6.3|       N|2021-07-06 00:00:00|      null|      null|
|-5.71735| 27.77937|     345.4|0.35| 0.57|        N|         nominal| 1.0NRT|     290.1|10.1|       N|2021-07-06 00:00:00|      null|      null|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
```