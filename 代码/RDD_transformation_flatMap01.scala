package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_flatMap01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_flatMap01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--flatMap
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val flatRDD = rdd.flatMap(
      x => x.split(" ")
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}