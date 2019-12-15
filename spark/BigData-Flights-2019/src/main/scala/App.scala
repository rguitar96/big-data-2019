import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics


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
      .load("data/1997_small.csv")

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
      .drop("CancellationCode")

    //See the remaining fields
    //    df
    //      .schema
    //      .fields
    //      .foreach(x => println(x))

    //Let's see how many rows are in the data frame.
    println("Total number of elements before filtering: "+df.count())

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
      .filter(df("FlightNum") =!= "NA")
      .filter(df("Cancelled") === 0)

    // Let's see how many rows are left.
    println("Total number of elements after filtering: "+df.count)

    //Since we only have the flights that were not cancelled, we can get rid of the Cancelled field:
    df = df
      .drop("Cancelled")

    //We will now change the data types of the appropriate fields from string to integer:
    df = df
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("DayOfMonth",col("DayOfMonth").cast(IntegerType))
      .withColumn("DayOfWeek",col("DayOfWeek").cast(IntegerType))
      .withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("CRSDepTime",col("CRSDepTime").cast(IntegerType))
      .withColumn("CRSArrTime",col("CRSArrTime").cast(IntegerType))
      .withColumn("DepTime",col("DepTime").cast(IntegerType))
      .withColumn("CRSElapsedTime",col("CRSElapsedTime").cast(IntegerType))
      .withColumn("ArrDelay",col("ArrDelay").cast(DoubleType))
      .withColumn("DepDelay",col("DepDelay").cast(IntegerType))
      .withColumn("Distance",col("Distance").cast(IntegerType))
      .withColumn("TaxiOut",col("TaxiOut").cast(IntegerType))


    // ADDING NEW COLUMNS
    df = df
      .withColumn("isWeekend", when(df.col("DayOfWeek") > 5, true) otherwise false)

    //This is how the data frame looks like now:
    //df.printSchema()

    df = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
      .withColumn("date", to_date(unix_timestamp($"merge", "yyyy-MM-dd").cast("timestamp")))
      .drop("merge")


    // TRANSFORMING DATA

    // Transform all cyclic data into sin/cos
    df = df
      .withColumn("DepTime_sin", sin((substring(col("DepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("DepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .withColumn("DepTime_cos", cos((substring(col("DepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("DepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .drop("DepTime")
      .withColumn("CRSDepTime_sin", sin((substring(col("CRSDepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSDepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .withColumn("CRSDepTime_cos", cos((substring(col("CRSDepTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSDepTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .drop("CRSDepTime")
      .withColumn("CRSArrTime_sin", sin((substring(col("CRSArrTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSArrTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .withColumn("CRSArrTime_cos", cos((substring(col("CRSArrTime"), 0, 2).cast(IntegerType) * 60 + substring(col("CRSArrTime"), 2, 2).cast(IntegerType)) * 2 * Math.PI / (24*60)))
      .drop("CRSArrTime")
      .withColumn("Month_sin", sin(col("Month") * 2 * Math.PI / 12))
      .withColumn("Month_cos", cos(col("Month") * 2 * Math.PI / 12))
      .drop("Month")
      .withColumn("DayOfMonth_sin", sin(col("DayOfMonth") * 2 * Math.PI / 31))
      .withColumn("DayOfMonth_cos", cos(col("DayOfMonth") * 2 * Math.PI / 31))
      .drop("DayOfMonth")
      .withColumn("DayOfWeek_sin", sin(col("DayOfWeek") * 2 * Math.PI / 7))
      .withColumn("DayOfWeek_cos", cos(col("DayOfWeek") * 2 * Math.PI / 7))
      .drop("DayOfWeek")

    df = df
      .filter($"DepTime_sin".isNotNull)
      .filter($"DepTime_cos".isNotNull)
      .filter($"CRSDepTime_sin".isNotNull)
      .filter($"CRSDepTime_cos".isNotNull)
      .filter($"CRSArrTime_sin".isNotNull)
      .filter($"CRSArrTime_cos".isNotNull)
      .filter($"Month_sin".isNotNull)
      .filter($"Month_cos".isNotNull)
      .filter($"DayOfMonth_sin".isNotNull)
      .filter($"DayOfMonth_cos".isNotNull)
      .filter($"DayOfWeek_sin".isNotNull)
      .filter($"DayOfWeek_cos".isNotNull)

    println("Total number of elements after filtering again: "+df.count)

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
    // df.printSchema()

    df.show(15)

    //Get state and city of airports

    val airports = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("sup_data/airports.csv")

    df = df.join(airports,
      df("Origin") === airports("iata"),
      "left")
      .drop("iata")
      .drop("airport")
      .drop("country")
      .drop("lat")
      .drop("long")
      .withColumnRenamed("city", "CityOrigion")
      .withColumnRenamed("state", "StateOrigion")

    df = df .join(airports,
      df("Dest") === airports("iata"),
      "left")
      .drop("iata")
      .drop("airport")
      .drop("country")
      .drop("lat")
      .drop("long")
      .withColumnRenamed("city", "CityDest")
      .withColumnRenamed("state", "StateDest")


    df.show()

    /////////////////////////////////////////
    // Part II: Creating the model
    //Make string indexers for string fetures
    val uniqueCarrierIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierIndex").setHandleInvalid("skip")
    val flightNumIndexer = new StringIndexer().setInputCol("FlightNum").setOutputCol("FlightNumIndex").setHandleInvalid("skip")
    val tailNumIndexer = new StringIndexer().setInputCol("TailNum").setOutputCol("TailNumIndex").setHandleInvalid("skip")
    val originIndexer = new StringIndexer().setInputCol("Origin").setOutputCol("OriginIndex").setHandleInvalid("skip")
    val destIndexer = new StringIndexer().setInputCol("Dest").setOutputCol("DestIndex").setHandleInvalid("skip")
    val cityOrigionIndexer = new StringIndexer().setInputCol("CityOrigion").setOutputCol("CityOrigionIndex").setHandleInvalid("skip")
    val stateOrigionIndexer = new StringIndexer().setInputCol("StateOrigion").setOutputCol("StateOrigionIndex").setHandleInvalid("skip")
    val cityDestIndexer = new StringIndexer().setInputCol("CityDest").setOutputCol("CityDestIndex").setHandleInvalid("skip")
    val stateDestIndexer = new StringIndexer().setInputCol("StateDest").setOutputCol("StateDestIndex").setHandleInvalid("skip")

    //Makes array of column names
    val colNames = Array("Year"
      , "DepTime_sin", "DepTime_cos"
      , "CRSDepTime_sin", "CRSDepTime_cos"
      , "CRSArrTime_sin", "CRSArrTime_cos"
      , "Month_sin", "Month_cos"
      , "DayOfMonth_sin", "DayOfMonth_cos"
      , "DayOfWeek_sin", "DayOfWeek_cos"
      , "CRSElapsedTime"
      , "DepDelay"
      , "Distance"
      , "TaxiOut"
      , "isWeekend"
      , "UniqueCarrierIndex"
      , "FlightNumIndex"
      , "TailNumIndex"
      , "OriginIndex"
      , "DestIndex"
      , "CityOrigionIndex"
      , "StateOrigionIndex"
      , "CityDestIndex"
      , "StateDestIndex"
    )


    val split = df.randomSplit(Array(0.7, 0.3))
    val training  = split(0)
    val test = split(1)

    training.show(100)

    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol("ArrDelay")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setTol(0.1)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline()
      .setStages(Array(
        uniqueCarrierIndexer, flightNumIndexer,tailNumIndexer, originIndexer, destIndexer, cityOrigionIndexer, stateOrigionIndexer, cityDestIndexer, stateDestIndexer,
        assembler,
        lr))

    println("Training....")
    val model = pipeline.fit(training)

    /////////////////////////////////////////
    // Part III: Validating the model

    println("Testing.....")

    val holdout = model.transform(test).select("prediction", "ArrDelay")

    val rm = new RegressionMetrics(holdout.rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

    println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")


  }
}
