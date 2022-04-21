package SparkCore.RDD的创建

import org.apache.spark.{SparkConf, SparkContext}

object RDD_create03 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf = new SparkConf().setAppName("RDD_create02").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //wholeTextFiles：以文件为单位读取数据，读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd = sc.wholeTextFiles("文件路径")
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
