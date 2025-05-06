import org.apache.spark.{SparkConf, SparkContext}

object WebLogProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WebLog").setMaster("local")
    val sc = new SparkContext(conf)

    // Updated file name to match your uploaded file
    val logs = sc.textFile("weblog.csv")

    val statusCodes = logs
      .map(_.split(" "))
      .filter(_.length > 8)
      .map(fields => (fields(8), 1))
      .reduceByKey(_ + _)

    println("Status Code Counts:")
    statusCodes.collect().foreach(println)

    sc.stop()
  }
}
