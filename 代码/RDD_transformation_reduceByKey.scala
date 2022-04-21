package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_reduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--reduceByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))
    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark基于scala开发，所以它的聚合也是两两聚合
    //reduceBykey中如果key的数据只有一个，是不会参与运算的
    val newRDD = rdd.reduceByKey(_+_)
    newRDD.collect().foreach(println)
  }
}