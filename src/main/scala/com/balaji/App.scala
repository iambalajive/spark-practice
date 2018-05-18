import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object SimpleApp {

  case class Person(name: String, gender: String, age: String)

  def dataSet(inputFile: String) = {
    val spark = SparkSession.builder().master("local").appName("dsds").getOrCreate()

    import spark.implicits._
    val ds = spark.read.csv(inputFile).toDF("name", "gender", "age")
      .as[Person]

    ds.foreach((x) =>{
      println(x.name)
    })


  }


  def dataframe(inputFile: String) = {
    val spark = SparkSession.builder().appName("Sample APP").getOrCreate()

    import spark.implicits._

    val df = spark.read.csv(inputFile).toDF("a", "b", "c", "d", "e", "f", "g")
    //    spark.sql("select count(b),a from stocks group by a having count(b) > 0").coalesce(1).write.csv("temp.csv")

    df.printSchema()
    df.select($"a", $"b" + 1).show()

    spark.close()


    //    val x = spark.sql("select a from stocks").foreach((x) => {
    //      println(x.getString(0))
    //    })


    //    df.show(2)
  }

  def average(sc: SparkContext, inputFile: String): Unit = {

    val input = sc.textFile(inputFile)

    //    Date,Open,High,Low,Close,Adj Close,Volume
    //Approach1

    val data = input.flatMap(_.split("\n")).map((x) => {
      val lineSplits = x.split(",")
      (lineSplits(0), lineSplits(1).toDouble)
    })

    data.persist()

    data.groupByKey().map((x) => (x._1, x._2.sum / x._2.size))

    //Approach2
    data.mapValues((x) => (x, 1)).reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    }).foreach(println)


  }

  def countAllOccurencesOfNums(sc: SparkContext, inputfile: String) = {
    val input = sc.textFile(inputfile)

    val x = input.flatMap(_.split("\n")).map((x) => {
      try {
        x.toInt
      } catch {
        case e: Exception => Int.MaxValue
      }
    }).filter(_ != Int.MaxValue).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    x.foreach(println)

  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("my app")
    val sc = new SparkContext(conf)

    //    countAllOccurencesOfNums(sc, "input.txt")

    //    average(sc, "yahoo-stock-data.csv")

    dataframe("yahoo-stock-data.csv")

    dataSet("person.csv")

  }
}