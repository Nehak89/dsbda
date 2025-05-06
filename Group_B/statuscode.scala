import org.apache.spark.{SparkConf, SparkContext}

object WebLogProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WebLog").setMaster("local")
    val sc = new SparkContext(conf)

    // Updated file name to match your uploaded file
    val logs = sc.textFile("weblog.csv")

    val statusCodes = logs
      .map(_.split(" "))
      .filter(_.length >=6)  //contains only 6 fields
      .map(fields => (fields.last, 1))  //status code is at last place
      .reduceByKey(_ + _)

    println("Status Code Counts:")
    statusCodes.collect().foreach(println)

    sc.stop()
  }
}
