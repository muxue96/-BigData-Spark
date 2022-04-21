package SparkCore.RDD的分区

import org.apache.spark.{SparkConf, SparkContext}

object RDD_partition01 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf = new SparkConf().setAppName("RDD_partition01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //RDD的并行度 & 分区
    //makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    /*第二个参数可以不传递的，那么makeRDD方法会使用默认值：(spark.default.parallelism,totalCores)
    spark在默认情况下，从配置对象conf中获取配置参数：spark.default.parallelism
    如果获取不到，那么使用totalCores,这个属性取值为当前环境的最大可用核数*/
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    //saveAsTextFile：将处理的数据保存成分区文件
    rdd.saveAsTextFile("file:///export/servers/spark/temp/sgg/partition1")
    //TODO 关闭环境
    sc.stop()
  }
}
