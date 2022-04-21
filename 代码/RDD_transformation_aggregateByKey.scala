package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_aggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--aggregateByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)
    //aggregateByKey存在函数颗粒化，有两个参数列表
    //第一个参数列表,需要 传递一个参数，表示为初始值，主要用于当碰到第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递2个参数
    //    第一个参数表示分区内计算规则
    //    第一个参数表示分区间计算规则
    //aggregateByKey最终返回的数据结果应该和初始值的类型保持一致->zeroValue
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x+y
    ).collect().foreach(println)
  }
}