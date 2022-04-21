package SparkCore.RDD的分区

import org.apache.spark.{SparkConf, SparkContext}

object RDD_partition02 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf = new SparkConf().setAppName("RDD_partition02").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //textFile可以将文件作为数据处理的数据源，默认也可以分区
    //如果不想使用默认的分区数量，可以通过第二个参数指定分区
    val rdd = sc.textFile("file:///export/servers/spark/temp/sgg/1.txt",2)
    //saveAsTextFile：将处理的数据保存成分区文件
    rdd.saveAsTextFile("file:///export/servers/spark/temp/sgg/partition2")
    //TODO 关闭环境
    sc.stop()
   }
}
