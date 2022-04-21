package SparkCore.RDD的创建
import org.apache.spark.{SparkConf, SparkContext}

object RDD_create02 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf = new SparkConf().setAppName("RDD_create02").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //textFile：以行为单位读取数据
    val rdd = sc.textFile("文件路径")
    /*path路径默认以当前环境的路径为基准，可以写绝对路径，也可以写相对路径
    pathye可以是本地文件系统路径：（file:///文件路径）
    path可以是分布式存储系统路径：HDFS（hdfs://主机名:9000/文件路径）
    path可以是文件的具体路径，也可以是目录名称，如果是目录名称会读取该目录下的所有文件
    path还可以使用通配符，例如：使用1*.txt可以匹配1.txt和11.txt*/
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
