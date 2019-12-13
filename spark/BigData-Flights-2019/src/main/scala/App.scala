import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.StandardScaler

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /////////////////////////////////////////
    // Part I: Data preparation

    val inputDf = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/1997.csv")

    // REMOVING COLUMNS AND MISSING VALUES
    var df = inputDf
      .drop("ArrTime")
      .drop("ActualElapsedTime")
      .drop("AirTime")
      .drop("TaxiIn")
      .drop("Diverted")
      .drop("CarrierDelay")
      .drop("WeatherDelay")
      .drop("NASDelay")
      .drop("SecurityDelay")
      .drop("LateAircraftDelay")

    df
      .schema
      .fields
      .foreach(x => println(x))


    // We drop CancellationCode for it is full of values (1997.csv).
    df = df
      .drop("CancellationCode")
    //See the remaining fields
    df
      .schema
      .fields
      .foreach(x => println(x))

    //Let's see how many rows are in the data frame.
    println("Total number of elements before filtering: "+df.count)

    //We remove the rows with missing values for the class (ArrDelay) since we can not used them for regression
    // purposes. We also filter out the rows with NA values for DepTime, DepDelay and CRSElapsedTime. The rows with
    // cancelled flies (Cancelled == 1) will also be eliminated. The latter match in number the rows with NA values for
    // columns DepTime and DepDelay. This makes sense and, although with one filter should be enough, we will filter
    // based on the three conditions to ensure that no NA values are left in the data.
    df = df
      .filter(df("ArrDelay") =!= "NA")
      .filter(df("DepTime") =!= "NA")
      .filter(df("DepDelay") =!= "NA")
      .filter(df("CRSElapsedTime") =!= "NA")
      .filter(df("Cancelled") === 0)

    // Let's see how many rows are left.
    println("Total number of elements after filtering: "+df.count)

    //Since we only have the flights that were not cancelled, we can get rid of the Cancelled field:
    df = df
      .drop("Cancelled")

    //We will now change the data types of the appropriate fields from string to integer:
    df = df
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("DayOfMonth",col("DayOfMonth").cast(IntegerType))
      .withColumn("DayOfWeek",col("DayOfWeek").cast(IntegerType))
      //.withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("CRSDepTime",col("CRSDepTime").cast(IntegerType))
      .withColumn("CRSArrTime",col("CRSArrTime").cast(IntegerType))
      .withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("FlightNum",col("FlightNum").cast(IntegerType))
      .withColumn("CRSElapsedTime",col("CRSElapsedTime").cast(IntegerType))
      .withColumn("ArrDelay",col("ArrDelay").cast(IntegerType))
      .withColumn("DepDelay",col("DepDelay").cast(IntegerType))
      .withColumn("Distance",col("Distance").cast(IntegerType))
      .withColumn("TaxiOut",col("TaxiOut").cast(IntegerType))


    // ADDING NEW COLUMNS
    df = df
        .withColumn("isWeekend", when(df.col("DayOfWeek") > 5, true) otherwise false)


    df = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
      .withColumn("date", to_date(unix_timestamp($"merge", "yyyy-MM-dd").cast("timestamp")))
      .drop("merge")


    // TRANSFORMING DATA

    // Transform all cyclic data into sin/cos
    df = df
      .withColumn("DepTime_sin", sin(((substring(col("DepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("DepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .withColumn("DepTime_cos", cos(((substring(col("DepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("DepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .drop("DepTime")
      .withColumn("CRSDepTime_sin", sin(((substring(col("CRSDepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSDepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .withColumn("CRSDepTime_cos", cos(((substring(col("CRSDepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSDepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .drop("CRSDepTime")
      .withColumn("CRSArrTime_sin", sin(((substring(col("CRSArrTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSArrTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .withColumn("CRSArrTime_cos", cos(((substring(col("CRSArrTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSArrTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60))))
      .drop("CRSArrTime")
      .withColumn("Month_sin", sin(col("Month") * 2 * Math.PI / (12)))
      .withColumn("Month_cos", cos(col("Month") * 2 * Math.PI / (12)))
      .drop("Month")
      .withColumn("DayOfMonth_sin", sin(col("DayOfMonth") * 2 * Math.PI / (31)))
      .withColumn("DayOfMonth_cos", cos(col("DayOfMonth") * 2 * Math.PI / (31)))
      .drop("DayOfMonth")
      .withColumn("DayOfWeek_sin", sin(col("DayOfWeek") * 2 * Math.PI / (7)))
      .withColumn("DayOfWeek_cos", cos(col("DayOfWeek") * 2 * Math.PI / (7)))
      .drop("DayOfWeek")

    // Normalize all numerical data
    df = df
      .select(min("CRSElapsedTime").alias("min_CRS"), max("CRSElapsedTime").alias("max_CRS"))
      .crossJoin(df)
      .withColumn("CRSElapsedTime" , (col("CRSElapsedTime") - col("min_CRS")) * 2 / (col("max_CRS") - col("min_CRS")) - 1)
          .drop("min_CRS")
          .drop("max_CRS")

    df = df
      .select(min("DepDelay").alias("min_delay"), max("DepDelay").alias("max_delay"))
      .crossJoin(df)
      .withColumn("DepDelay" , (col("DepDelay") - col("min_delay")) * 2 / (col("max_delay") - col("min_delay")) - 1)
      .drop("min_delay")
      .drop("max_delay")

    df = df
      .select(min("Distance").alias("min_distance"), max("Distance").alias("max_distance"))
      .crossJoin(df)
      .withColumn("Distance" , (col("Distance") - col("min_distance")) * 2 / (col("max_distance") - col("min_distance")) - 1)
      .drop("min_distance")
      .drop("max_distance")

    df = df
      .select(min("TaxiOut").alias("min_taxi"), max("TaxiOut").alias("max_taxi"))
      .crossJoin(df)
      .withColumn("TaxiOut" , (col("TaxiOut") - col("min_taxi")) * 2 / (col("max_taxi") - col("min_taxi")) - 1)
      .drop("min_taxi")
      .drop("max_taxi")

    //This is how the data frame looks like now:
    df.printSchema()

    df
      .show(15)

  }
}
