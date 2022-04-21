package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_cogroup").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--cogroup
    val rdd1 = sc.makeRDD(List(("a",1),("b",2)))

    val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6),("c",7)))
    //cogroup：connect(连接) + group(分组),最多可以连接三个rdd的连接
    val cgRDD = rdd1.cogroup(rdd2)
    cgRDD.foreach(println)
  }
}