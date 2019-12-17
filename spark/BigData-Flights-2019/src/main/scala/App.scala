import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator




object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    /////////////////////////////////////////
    // Part I: Data preparation

    println("Getting data")

    val inputDf = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/1997.csv")

    var df = inputDf
      .drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "CancellationCode")

    println("Got data")

    //Let's see how many rows are in the data frame.
    println("Total number of elements before filtering: "+df.count())

    //We remove the rows with missing values for the class (ArrDelay) since we can not used them for regression
    // purposes. We also filter out the rows with NA values for DepTime, DepDelay and CRSElapsedTime. The rows with
    // cancelled flies (Cancelled == 1) will also be eliminated. The latter match in number the rows with NA values for
    // columns DepTime and DepDelay. This makes sense and, although with one filter should be enough, we will filter
    // based on the three conditions to ensure that no NA values are left in the data.

    df = df
      .filter($"ArrDelay" =!= "NA")
      .filter($"DepDelay" =!= "NA")
      .filter($"CRSElapsedTime" =!= "NA")
      .filter($"Cancelled" === 0)

    //Since we only have the flights that were not cancelled, we can get rid of the Cancelled field:
    df = df
      .drop("Cancelled")

    // Let's see how many rows are left.
    println("Total number of elements after filtering: "+df.count)

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
      .withColumn("TaxiOut",when(col("TaxiOut").isNull, 15) otherwise col("TaxiOut"))


    // ADDING NEW COLUMNS
    println("Adding columns")

    df = df
      .withColumn("isWeekend", when(df.col("DayOfWeek") > 5, true) otherwise false)

    df = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
      .withColumn("date", to_date(unix_timestamp($"merge", "yyyy-MM-dd").cast("timestamp")))
      .drop("merge")

    // TRANSFORMING DATA
    println("Transforming data")


    df = df.withColumn("DepTime",when(col("DepTime") < 1000, 1200) otherwise col("DepTime"))
    df = df.withColumn("CRSDepTime",when(col("CRSDepTime") < 1000, 1200) otherwise col("CRSDepTime"))
    df = df.withColumn("CRSArrTime",when(col("CRSArrTime") < 1000, 1200) otherwise col("CRSArrTime"))

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
      //.withColumn("Month_sin", sin(col("Month") * 2 * Math.PI / 12))
      //.withColumn("Month_cos", cos(col("Month") * 2 * Math.PI / 12))
      .drop("Month")
      //.withColumn("DayOfMonth_sin", sin(col("DayOfMonth") * 2 * Math.PI / 31))
      //.withColumn("DayOfMonth_cos", cos(col("DayOfMonth") * 2 * Math.PI / 31))
      .drop("DayOfMonth")
      //.withColumn("DayOfWeek_sin", sin(col("DayOfWeek") * 2 * Math.PI / 7))
      //.withColumn("DayOfWeek_cos", cos(col("DayOfWeek") * 2 * Math.PI / 7))
      .drop("DayOfWeek")

    df = df.withColumn("TaxiOut",when(col("TaxiOut").isNull, 15) otherwise col("TaxiOut"))

    println("Total number of elements before training: "+df.count)

    /////////////////////////////////////////
    // Part II: Creating the model
    //Make string indexers for string fetures
    //val uniqueCarrierIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierIndex").setHandleInvalid("skip")
    val originIndexer = new StringIndexer().setInputCol("Origin").setOutputCol("OriginIndex").setHandleInvalid("skip")
    //val destIndexer = new StringIndexer().setInputCol("Dest").setOutputCol("DestIndex").setHandleInvalid("skip")
    //val cityOriginIndexer = new StringIndexer().setInputCol("CityOrigin").setOutputCol("CityOriginIndex").setHandleInvalid("skip")
    //val stateOriginIndexer = new StringIndexer().setInputCol("StateOrigin").setOutputCol("StateOriginIndex").setHandleInvalid("skip")
    //val cityDestIndexer = new StringIndexer().setInputCol("CityDest").setOutputCol("CityDestIndex").setHandleInvalid("skip")
    //val stateDestIndexer = new StringIndexer().setInputCol("StateDest").setOutputCol("StateDestIndex").setHandleInvalid("skip")

    //Makes array of column names
    val colNames = Array(
       "DepDelay"
      , "TaxiOut"
      //, "isWeekend"
      //,"DepTime_sin", "DepTime_cos"
      //, "CRSDepTime_sin", "CRSDepTime_cos"
      //, "CRSArrTime_sin", "CRSArrTime_cos"
      //, "OriginIndex"
      //, "date"
    )

    val split = df.randomSplit(Array(0.7, 0.3))
    val training  = split(0)
    val test = split(1)

    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val lr = new LinearRegression()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline()
      .setStages(Array(
        //originIndexer,
        assembler,
        scaler,
        lr))


    println("Training....")
    val model = pipeline.fit(training)

    /////////////////////////////////////////
    // Part III: Validating the model

    println("Testing.....")

    val predRes = model.transform(test)

    predRes.show(15)

    val regEval = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("ArrDelay")

    println("R2: "+regEval.setMetricName("r2").evaluate(predRes))
    println("MSE: "+regEval.setMetricName("mse").evaluate(predRes))
    println("RMSE: "+regEval.setMetricName("rmse").evaluate(predRes))

    val linearModel = model.stages(2).asInstanceOf[LinearRegressionModel]

    println(s"Coefficients: ${linearModel.coefficients} Intercept: ${linearModel.intercept}")
  }
}
