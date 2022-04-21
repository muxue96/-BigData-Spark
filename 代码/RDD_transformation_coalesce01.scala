package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_coalesce01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_coalesce01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--coalesce
    //coalesce方法是可以扩大分区的，但是如果不进行shuffle处理，是没有意义的，不起作用
    //所以想要实现扩大分区的效果，需要使用shuffle操作
    //spark提供了一个简化的操作
    //缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    //扩大分区：repartition，底层代码调用的就是coalesce，而且肯定采用shuffle
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)
    val newRDD = rdd.coalesce(3,true)
    newRDD.saveAsTextFile("file:///export/servers/spark/temp/sgg/coalesce2")
    sc.stop()
  }
}