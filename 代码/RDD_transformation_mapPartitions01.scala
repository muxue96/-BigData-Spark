package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_mapPartitions01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_mapPartitions01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--mapPartitions
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val makeRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    makeRDD.collect.foreach(println)
    sc.stop()
  }
}