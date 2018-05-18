package com.balaji

import org.apache.spark.{SparkConf, SparkContext}


class PairRDD {

}


object PairRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("my app")
    val x = new SparkContext(conf)

  }
}
