package SparkCore.RDD的序列化

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_serial {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_transformation_serial")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search = new Search(query ="h")
    search.getMatch1(rdd).collect().foreach(println)

    sc.stop()
  }
}
//类的构造参数其实是类的属性，构造参数需要进行闭包检测，其实就等同于类进行闭包检测
class Search(query:String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }
  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch)
  }
  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query))
    //val q = query
    //rdd.filter(x => x.contains(q))
  }
}

