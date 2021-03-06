package com.dt.spark.cores

import org.apache.spark.{SparkConf, SparkContext}

object MovieUsersAnalyzerRDD {

  val dataPath = "/Users/chenchen/bitnei/sparkapps/data/"

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Movie_Users_Analyzer."))
    val userRDD = sc.textFile(dataPath + "users.csv")
    val res: Array[String] = userRDD.collect()

    res.map(x=> println(x))
  }

}
