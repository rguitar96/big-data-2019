import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /////////////////////////////////////////
    // Exercise 1

    val inputDf = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("data/1997.csv")

    val df = inputDf
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
    val df = df
      .drop("Year")
      .drop("CancellationCode")
    //See the remaining fields
    df
      .schema
      .fields
      .foreach(x => println(x))

    //Let's see how many rows are in the data frame.
    df.count()

    //We remove the rows with missing values for the class (ArrDelay) since we can not used them for regression
    // purposes. We also filter out the rows with NA values for DepTime, DepDelay and CRSElapsedTime. The rows with
    // cancelled flies (Cancelled == 1) will also be eliminated. The latter match in number the rows with NA values for
    // columns DepTime and DepDelay. This makes sense and, although with one filter should be enough, we will filter
    // based on the three conditions to ensure that no NA values are left in the data.
    val df = df
      .filter(df("ArrDelay") =!= "NA")
      .filter(df("DepTime") =!= "NA")
      .filter(df("DepDelay") =!= "NA")
      .filter(df("CRSElapsedTime") =!= "NA")
      .filter(df("Cancelled") === 1)

    // Let's see how many rows are left.
    df.count()

    /*
        inputDf.printSchema()
        inputDf.show(15, truncate = false)

        /////////////////////////////////////////
        // Exercise 2

        println("Total number of elements: "+inputDf.count)

        println("Complete list of project names")
        val projectList = inputDf
          .select("project_name")
          .distinct
          .rdd
          .map((r: Row) => r.getString(0))
          .collect
        println(projectList)

        inputDf
          .filter(col("project_name") === "en")
          .groupBy("project_name")
          //.sum("content_size")
          .agg(sum("content_size").as("total_size"))
          .show(truncate = false)

        inputDf
          .filter(col("project_name") === "en")
          .orderBy(col("num_requests").desc)
          .show(5, truncate = false)

        // Now with SQL
        inputDf.createOrReplaceTempView("myFakeTable")
        spark.sql("select count(*) from myFakeTable").show
        spark.sql("select distinct project_name from myFakeTable").show
        spark.sql("select sum(content_size) from myFakeTable where project_name = 'en'").show
        spark.sql("select * from myFakeTable where project_name = 'en' order by num_requests desc limit 5").show

        /////////////////////////////////////////
        // Exercise 3

        println("Project summary")

        val projectSummary = inputDf
          .groupBy("project_name")
          .agg(
            count("page_title").as("num_pages"),
            sum("content_size").as("content_size"),
            mean("num_requests").as("mean_requests"))

        projectSummary.show(projectList.length, truncate = false)

        println("Most visited")
        inputDf
          .join(projectSummary.select("project_name", "mean_requests"), "project_name")
          .filter(col("num_requests") > col("mean_requests"))
          .show

     */
  }
}
