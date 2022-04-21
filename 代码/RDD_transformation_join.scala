package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_join").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--join
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    val rdd2 = sc.makeRDD(List(("a",4),("c",5),("a",6)))
    //join：两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //      如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    //      如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔积乘积，数据会几何性增长，会导致性能降低
    val joinRDD = rdd1.join(rdd2)

    joinRDD.foreach(println)
  }
}