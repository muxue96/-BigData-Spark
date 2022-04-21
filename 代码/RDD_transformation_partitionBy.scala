package SparkCore.RDD转换算子

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--partitionBy
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd.map((_,1))
    //隐式转换（二次编译）

    //partitionBy根据指定的分区规则对数据进行重分区
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("file:///export/servers/spark/temp/sgg/partitionBy")

  }
}