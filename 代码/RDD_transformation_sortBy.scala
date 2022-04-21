package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_sortBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--sortBy
    val rdd = sc.makeRDD(List(6,2,1,4,5,3 ),2)
    val sortRDD = rdd.sortBy(num => num)
    sortRDD.saveAsTextFile("file:///export/servers/spark/temp/sgg/RDD_transformation_sortBy")
    sc.stop()
  }
}