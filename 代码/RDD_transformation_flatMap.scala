package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_flatMap").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--flatMap
    val rdd = sc.makeRDD(List(List(1,2),List(3,4)))
    val flatRDD = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}