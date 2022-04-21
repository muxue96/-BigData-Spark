package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_distinct").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--distinct
    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))
    val rdd1 =rdd.distinct()
    rdd1.collect().foreach(println)
    sc.stop()
  }
}