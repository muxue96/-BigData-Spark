package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_coalesce").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--coalesce
    //coalesce方法默认情况下是不会将分区的数据打乱重新组合的
    //这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想要让数据均衡，可以进行shuffle处理
    //coalesce的第二个参数默认为false，不进行shuffle处理，改为true则会进行shuffle处理
    val rdd = sc.makeRDD(List(1,2,3,4),4)
    val newRDD = rdd.coalesce(2)
    newRDD.saveAsTextFile("file:///export/servers/spark/temp/sgg/coalesce")
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),3)
    val newRDD1 = rdd1.coalesce(2,true)
    newRDD1.saveAsTextFile("file:///export/servers/spark/temp/sgg/coalesce1")
    sc.stop()
  }
}