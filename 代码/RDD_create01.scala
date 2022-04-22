package SparkCore.RDD的创建

import org.apache.spark.{SparkConf, SparkContext}

object RDD_create01 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf = new SparkConf().setAppName("RDD_create01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq = Seq[Int](1,2,3,4)
    val rdd = sc.parallelize(seq)
    //parallelize:并行
    rdd.collect().foreach(println)
    //makeRDD方法在底层实现时其实就是调用了rdd对象的parallelize方法，效果和parallelize一样
    val rdd2 = sc.makeRDD(seq)
    rdd2.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}
1111
