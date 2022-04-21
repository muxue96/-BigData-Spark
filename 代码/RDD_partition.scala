package SparkCore.RDD的分区

import org.apache.spark.{SparkConf, SparkContext}

object RDD_partition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_partition").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd.map(
      num => {
        print(">>>>>>"+num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        print("======"+num)
        num
        //        1.rdd的计算一个分区内 的数据是一个一个执行逻辑的，只有前面一个数据全部的逻辑执行完毕后，下一个数据才会执行逻辑，分区内的数据的执行是有序的
        //        2.不同分区数据计算是无序的
      }
    )
  }
}
