import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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


    //We decide to remove year because it always has the same value. We drop CancellationCode for it is full of NA
    // values (1997.csv).
    df = df
      .drop("Year")
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
      .withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("CRSDepTime",col("CRSDepTime").cast(IntegerType))
      .withColumn("CRSArrTime",col("CRSArrTime").cast(IntegerType))
      .withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("FlightNum",col("FlightNum").cast(IntegerType))
      .withColumn("CRSElapsedTime",col("CRSElapsedTime").cast(IntegerType))
      .withColumn("ArrDelay",col("ArrDelay").cast(IntegerType))
      .withColumn("DepDelay",col("DepDelay").cast(IntegerType))
      .withColumn("Distance",col("Distance").cast(IntegerType))
      .withColumn("TaxiOut",col("TaxiOut").cast(IntegerType))


    df = df
        .withColumn("isWeekend", when(df.col("DayOfWeek") > 5, true) otherwise false)

    //This is how the data frame looks like now:
    df.printSchema()

    df
      .filter(df("DayOfWeek") >= 5)
      .show(5)
  }
}
