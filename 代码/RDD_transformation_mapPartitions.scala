package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_mapPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //mapPartitions：可以以分区为单位进行数据转换操作
    //               但是会将整个分区的数据加载到内存中进行引用
    //               处理完的数据是不会被释放掉的，存在对象的调用（因为是以分区为单位的，进来的时候的时候是一个迭代器，处理完第一条，后面的还没处理完，所以没法释放）
    //               在内存中，数据量较大的场合下，容易出现内存溢出
    val makeRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>")
        iter.map(_*2)
      }
    )
/*
    map 和 mapPartitions 的区别？
    ➢ 数据处理角度
      Map 算子是分区内一个数据一个数据的执行，类似于串行操作。而 mapPartitions 算子
    是以分区为单位进行批处理操作。
    ➢ 功能的角度
      Map 算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。
    MapPartitions 算子需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不变，
    所以可以增加或减少数据
    ➢ 性能的角度
      Map 算子因为类似于串行操作，所以性能比较低，而是 mapPartitions 算子类似于批处
    理，所以性能较高。但是 mapPartitions 算子会长时间占用内存，那么这样会导致内存可能
    不够用，出现内存溢出的错误。所以在内存有限的情况下，不推荐使用。使用 map 操作。
*/
    sc.stop()
  }
}