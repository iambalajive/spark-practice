import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
   val conf = new SparkConf().setMaster("local").setAppName("my app")
    val sc = new SparkContext(conf)

    // Load our input data.
    val input = sc.textFile("README.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output.txt")

  }
}