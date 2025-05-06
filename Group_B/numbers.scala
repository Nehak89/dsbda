val inputfile = sc.textFile("/home/student/DSBDAL/numbers.txt")

val classifiedNumbers = inputfile .flatMap(line => line.split(" ")) .map(numStr => numStr.toInt).map(num => {
    if (num > 0) {
      ("positive", 1)
    } else if (num < 0) {
      ("negative", 1)
    } else {
      ("zero", 1)
    }
  })
  .reduceByKey(_ + _)                     

println(classifiedNumbers.toDebugString)
classifiedNumbers.cache()
classifiedNumbers.saveAsTextFile("output")
