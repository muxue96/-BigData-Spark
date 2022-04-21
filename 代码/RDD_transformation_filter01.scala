package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_filter01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_filter01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--filter
    val rdd =sc.textFile("file:///export/servers/spark/temp/sgg/apache.log")
    rdd.filter(
      line => {
        val datas = line.split(" ")
        val time =datas(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)
    sc.stop()
  }
}