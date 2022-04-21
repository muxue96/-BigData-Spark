package SparkCore.RDD转换算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_glom01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_glom01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--glom
    val rdd : RDD[Int]= sc.makeRDD(List(1,2,3,4),2)
    val glomRDD = rdd.glom()
    val maxRDD = glomRDD.map(
      array => array.max
    )
    println(maxRDD.collect().sum)
    sc.stop()
  }
}