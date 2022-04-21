package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_map01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_map01").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO算子 - map
    val rdd = sc.textFile("文件路径")
    val mapRDD = rdd.map(
      line => line.split(" ")(6)
    )
    sc.stop()
  }
}
