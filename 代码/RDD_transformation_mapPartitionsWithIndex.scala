package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_mapPartitionsWithIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val makeRDD = rdd.mapPartitionsWithIndex(
      (index,iter) => {
        if (index == 1){
          iter
        }else{
          Nil.iterator//返回空的迭代器
        }
      }
    )
    sc.stop()
  }
}