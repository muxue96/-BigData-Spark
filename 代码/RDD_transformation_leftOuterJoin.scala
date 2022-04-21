package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_leftOuterJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_leftOuterJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--leftOuterJoin和rightOuterJoin
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    val rdd2 = sc.makeRDD(List(("a",4),("b",5)))

    val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
    val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

    leftJoinRDD.foreach(println)
    rightJoinRDD.foreach(println)

  }
}