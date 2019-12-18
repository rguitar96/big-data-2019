import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder

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
      .load("data/inputData.csv")

    var df = inputDf
      .drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "CancellationCode")


    /*Data exploration
println("Let's see the summary statistics of the data!")
df.describe().show()

println("We have several NAs values :( . But how many?")
df.groupBy("Year").count().show()
df.filter(df("Year") === "NA").count()
df.groupBy("Month").count().show()
df.filter(df("Month") === "NA").count()
df.groupBy("DayOfMonth").count().show()
df.filter(df("DayOfMonth") === "NA").count()
df.groupBy("DayOfWeek").count().show()
df.filter(df("DayOfWeek") === "NA").count()
df.groupBy("DepTime").count().show()
df.filter(df("DepTime") === "NA").count()
df.groupBy("CRSDepTime").count().show()
df.filter(df("CRSDepTime") === "NA").count()
df.groupBy("CRSArrTime").count().show()
df.filter(df("CRSArrTime") === "NA").count()
df.groupBy("UniqueCarrier").count().show()
df.filter(df("UniqueCarrier") === "NA").count()
df.groupBy("FlightNum").count().show()
df.filter(df("FlightNum") === "NA").count()
df.groupBy("TailNum").count().show()
df.filter(df("TailNum") === "NA").count()
df.groupBy("CRSElapsedTime").count().show()
df.filter(df("CRSElapsedTime") === "NA").count()
df.groupBy("ArrDelay").count().show()
df.filter(df("ArrDelay") === "NA").count()
df.groupBy("DepDelay").count().show()
df.filter(df("DepDelay") === "NA").count()
df.groupBy("Origin").count().show()
df.filter(df("Origin") === "NA").count()
df.groupBy("Dest").count().show()
df.filter(df("Dest") === "NA").count()
df.groupBy("Distance").count().show()
df.filter(df("Distance") === "NA").count()
df.groupBy("TaxiOut").count().show()
df.filter(df("TaxiOut") === "NA").count()
df.groupBy("Cancelled").count().show()
df.filter(df("Cancelled") === "NA").count()
df.groupBy("CancellationCode").count().show()
df.filter(df("CancellationCode") === "NA").count()

//Filter out NA values
df = df.filter(df("Year") =!= "NA")
df = df.filter(df("Month") =!= "NA")
df = df.filter(df("DayOfMonth") =!= "NA")
df = df.filter(df("DayOfWeek") =!= "NA")
df = df.filter(df("DepTime") =!= "NA")
df = df.filter(df("CRSDepTime") =!= "NA")
df = df.filter(df("CRSArrTime") =!= "NA")
df = df.filter(df("UniqueCarrier") =!= "NA")
df = df.filter(df("FlightNum") =!= "NA")
df = df.filter(df("TailNum") =!= "NA")
df = df.filter(df("CRSElapsedTime") =!= "NA")
df = df.filter(df("ArrDelay") =!= "NA")
df = df.filter(df("DepDelay") =!= "NA")
df = df.filter(df("Origin") =!= "NA")
df = df.filter(df("Dest") =!= "NA")
df = df.filter(df("Distance") =!= "NA")
df = df.filter(df("TaxiOut") =!= "NA")


//Filter out null values
df = df.filter(df("Year").isNotNull)
df = df.filter(df("Month").isNotNull)
df = df.filter(df("DayOfMonth").isNotNull)
df = df.filter(df("DayOfWeek").isNotNull)
df = df.filter(df("DepTime").isNotNull)
df = df.filter(df("CRSDepTime").isNotNull)
df = df.filter(df("CRSArrTime").isNotNull)
df = df.filter(df("UniqueCarrier").isNotNull)
df = df.filter(df("FlightNum").isNotNull)
df = df.filter(df("TailNum").isNotNull)
df = df.filter(df("CRSElapsedTime").isNotNull)
df = df.filter(df("ArrDelay").isNotNull)
df = df.filter(df("DepDelay").isNotNull)
df = df.filter(df("Origin").isNotNull)
df = df.filter(df("Dest").isNotNull)
df = df.filter(df("Distance").isNotNull)

println("Getting supplementary data")

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

//Get plane features
val planes = spark
.read
.format("csv")
.option("header", "true")
.load("sup_data/plane-data.csv")
.withColumnRenamed("tailnum", "planeTailNum")
.withColumnRenamed("year", "manufacturedYear")

df = df .join(planes,
df("TailNum") === planes("planeTailNum"),
"left")
.drop("planeTailNum")
.drop("type")
.drop("issue_date")
.drop("status")
.drop("engine_type")
.withColumn("PlaneAge", col("Year") - col("manufacturedYear").cast(IntegerType) )
.drop("manufacturedYear")

df = df
.select(floor(avg("PlaneAge")).alias("avg_PlaneAge"))
.withColumn("avg_PlaneAge", when(col("avg_PlaneAge").isNull, 10) otherwise(col("avg_PlaneAge")))
.crossJoin(df)
.withColumn("PlaneAge",when(col("PlaneAge").isNull, col("avg_PlaneAge")) otherwise col("PlaneAge"))
.drop(col("avg_PlaneAge"))
*/

    println("Got data")

    //Let's see how many rows are in the data frame.
    println("Total number of elements before filtering: "+df.count())

    //Since we only have the flights that were not cancelled, we can get rid of the Cancelled field:
    df = df
      .drop("Cancelled")

    //We will now change the data types of the appropriate fields from string to integer:
    df = df
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("DayOfMonth",col("DayOfMonth").cast(IntegerType))
      .withColumn("DayOfWeek",col("DayOfWeek").cast(IntegerType))
      //.withColumn("DepTime",col("DepTime").cast(IntegerType))
      //.withColumn("CRSDepTime",col("CRSDepTime").cast(IntegerType))
      //.withColumn("CRSArrTime",col("CRSArrTime").cast(IntegerType))
      .withColumn("CRSElapsedTime",col("CRSElapsedTime").cast(IntegerType))
      .withColumn("ArrDelay",col("ArrDelay").cast(DoubleType))
      .withColumn("DepDelay",col("DepDelay").cast(IntegerType))
      .withColumn("Distance",col("Distance").cast(IntegerType))
      .withColumn("TaxiOut",col("TaxiOut").cast(IntegerType))
      .withColumn("TaxiOut",when(col("TaxiOut").isNull, 15) otherwise col("TaxiOut"))

    // We cannot predict results if the response variable is null
    df = df.filter(col("ArrDelay").isNotNull)

    // ADDING NEW COLUMNS
    println("Adding columns")

    df = df
      .withColumn("isWeekend", when(df.col("DayOfWeek") > 5, true) otherwise false)

    df = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
      .withColumn("date", unix_timestamp($"merge", "yyyy-MM-dd"))
      .drop("merge")

    // TRANSFORMING DATA
    println("Transforming data")

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
      .withColumn("DayOfWeek_sin", sin(col("DayOfWeek") * 2 * Math.PI / 7))
      //.withColumn("DayOfWeek_cos", cos(col("DayOfWeek") * 2 * Math.PI / 7))
      .drop("DayOfWeek")

    df = df.withColumn("TaxiOut",when(col("TaxiOut").isNull, 15) otherwise col("TaxiOut"))

    println("Total number of elements before training: "+df.count)

    df.show(15)


    /*Correlations
df.stat.corr("ArrDelay","Year")
df.stat.corr("ArrDelay","Month")
df.stat.corr("ArrDelay","DayOfMonth")
df.stat.corr("ArrDelay","DayOfWeek")
df.stat.corr("ArrDelay","DepTime")
df.stat.corr("ArrDelay","CRSDepTime")
df.stat.corr("ArrDelay","CRSArrTime")
df.stat.corr("ArrDelay","UniqueCarrier")
df.stat.corr("ArrDelay","FlightNum")
df.stat.corr("ArrDelay","TailNum")
df.stat.corr("ArrDelay","CRSElapsedTime")
df.stat.corr("ArrDelay","DepDelay")
df.stat.corr("ArrDelay","Origin")
df.stat.corr("ArrDelay","Dest")
df.stat.corr("ArrDelay","Distance")
df.stat.corr("ArrDelay","TaxiOut")
df.stat.corr("ArrDelay","Date")
df.stat.corr("ArrDelay","isWeekend")
df.stat.corr("ArrDelay","DepTime_sin")
df.stat.corr("ArrDelay","DepTime_cos")
df.stat.corr("ArrDelay","CRSDepTime_sin")
df.stat.corr("ArrDelay","CRSDepTime_cos")
df.stat.corr("ArrDelay","CRSArrTime_sin")
df.stat.corr("ArrDelay","CRSArrTime_cos")
df.stat.corr("ArrDelay","Month_sin")
df.stat.corr("ArrDelay","Month_cos")
df.stat.corr("ArrDelay","DayOfMonth_sin")
df.stat.corr("ArrDelay","DayOfMonth_cos")
df.stat.corr("ArrDelay","DayOfWeek_sin")
df.stat.corr("ArrDelay","DayOfWeek_cos")
df.stat.corr("ArrDelay","UniqueCarrierIndex")
df.stat.corr("ArrDelay","OriginIndex")
df.stat.corr("ArrDelay","DestIndex")
df.stat.corr("ArrDelay","CityIndex")
df.stat.corr("ArrDelay","StateIndex")
*/


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

    //Make string one hot encoders for random forest
    val originEnconder = new OneHotEncoderEstimator().setInputCols(Array("OriginIndex")).setOutputCols(Array("OriginIndexEncoded"))

    //Make scaler for models
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    //split data for training and testing
    val split = df.randomSplit(Array(0.7, 0.3))
    val training  = split(0)
    val test = split(1)

    //Linear regression model
    println("Setting up linear regression model")

    //Makes array of column names
    val lrColNames = Array(
       "DepDelay"
      , "TaxiOut"
      , "DepTime_sin", "DepTime_cos"
      , "DayOfWeek_sin"
      , "OriginIndex"
      , "date"
    )

    val lrAssembler = new VectorAssembler()
      .setInputCols(lrColNames)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val lr = new LinearRegression()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrPipeline = new Pipeline()
      .setStages(Array(
        originIndexer,
        lrAssembler,
        scaler,
        lr))


    println("Training....")
    val lrModel = lrPipeline.fit(training)

    /////////////////////////////////////////
    // Part III: Validating the model

    println("Testing.....")

    val lrPredRes = lrModel.transform(test)

    val lrRegEval = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("ArrDelay")

    println("Linear regression results")
    println("R2: "+lrRegEval.setMetricName("r2").evaluate(lrPredRes))
    println("MSE: "+lrRegEval.setMetricName("mse").evaluate(lrPredRes))
    println("RMSE: "+lrRegEval.setMetricName("rmse").evaluate(lrPredRes))

    val linearModel = lrModel.stages(3).asInstanceOf[LinearRegressionModel]

    println(s"Coefficients: ${linearModel.coefficients} Intercept: ${linearModel.intercept}")

    //Random forest
    println("Setting up random forest model")

    //Makes array of column names
    val rfColNames = Array(
      "DepDelay"
      , "TaxiOut"
      , "DepTime_sin", "DepTime_cos"
      , "DayOfWeek_sin"
      ,"OriginIndexEncoded"
      , "date"
    )

    val rfAssembler = new VectorAssembler()
      .setInputCols(rfColNames)
      .setOutputCol("features")
      .setHandleInvalid("skip")


    val rf = new RandomForestRegressor()
      .setLabelCol("ArrDelay")
      .setFeaturesCol("features")
      .setNumTrees(8)
      .setMaxBins(28)
      .setMaxDepth(8)


    val rfPipeline = new Pipeline()
      .setStages(Array(
        originIndexer,
        originEnconder,
        rfAssembler,
        scaler,
        rf))

    println("Training....")
    val rfModel = rfPipeline.fit(training)

    /////////////////////////////////////////
    // Part III: Validating the model

    println("Testing.....")

    val rfPredRes = rfModel.transform(test)


    val rfRegEval = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("ArrDelay")

    println("Random forest results")
    println("R2: "+rfRegEval.setMetricName("r2").evaluate(rfPredRes))
    println("MSE: "+rfRegEval.setMetricName("mse").evaluate(rfPredRes))
    println("RMSE: "+rfRegEval.setMetricName("rmse").evaluate(rfPredRes))

    spark.stop()
  }
}

