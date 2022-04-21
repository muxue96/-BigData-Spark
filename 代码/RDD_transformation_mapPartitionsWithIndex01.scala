package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_mapPartitionsWithIndex01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RRDD_transformation_mapPartitionsWithIndex01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1,2,3,4))
    val makeRDD = rdd.mapPartitionsWithIndex(
      (index,iter) => {
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )
    sc.stop()
  }
}