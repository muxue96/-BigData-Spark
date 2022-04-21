package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_aggregateByKey01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_aggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--aggregateByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

    //aggregateByKey最终返回的数据结果应该和初始值的类型保持一致->zeroValue
    //获取相同key的数据的平均值 => (a,3),(b,4)
    val newRDD = rdd.aggregateByKey((0,0))(
      (t,v) => {
        (t._1+v, t._2+1)
      },
      (t1,t2) => {
        (t1._1 + t2._1,t1._2 + t2._2)
      }
     )
    newRDD.mapValues{
      case (num,count) =>
        num/count
    }
  }
}