package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object RDD_transformation_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_filter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--filter
    val rdd = sc.makeRDD(List(1,2,3,4))
    val filterRDD = rdd.filter(num => num % 2 != 0)
    filterRDD.collect.foreach(println)
    sc.stop()
  }
}