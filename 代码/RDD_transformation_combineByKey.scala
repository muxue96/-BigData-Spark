package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_combineByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--combineByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)),2)
    //combineByKey：方法需要三个参数
    //第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
    //第二个参数表示：分区内的计算规则
    //第三个参数表示：分区间的计算规则
    //获取相同key的数据的平均值 => (a,3),(b,4)
    val newRDD = rdd.combineByKey(
      v => (v,1),
      (t:(Int,Int),v) => {
        (t._1+v, t._2+1)
      },
      (t1:(Int,Int),t2:(Int,Int)) => {
        (t1._1 + t2._1,t1._2 + t2._2)
      }
     )
    newRDD.mapValues{
      case (num,count) =>
        num/count
    }
  }
}