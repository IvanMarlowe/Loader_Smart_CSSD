
import helper.OutputGenerator
import helper.DataFrameHelper
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{IntWritable, Text, BytesWritable}
import helper.DataManipulator
import java.nio.charset.StandardCharsets

object SourceGenerator{
  def main(args: Array[String]) {
    val listFileLocation = args(0)
    val cdrType = args(1)
    val outputTgtLocation = args(2)
    val listData = DataFrameHelper.readListFile(listFileLocation)
    val emptyRDD:RDD[String] = ContextHelper.getSparkContext().emptyRDD
    val sequenceList = listData.map(seqLoc => {
      val sequenceList = ContextHelper.getSparkContext().sequenceFile(seqLoc, classOf[IntWritable], classOf[BytesWritable], DataManipulator.getTotalCoresTask)
      sequenceList
      .flatMap(data => new String(data._2.copyBytes(), StandardCharsets.UTF_8).split("\n"))
      
    })
    .foldLeft(emptyRDD)((x, y) => x.union(y))
    .repartition(DataManipulator.getTotalCoresTask * 2)
    
    sequenceList.saveAsTextFile(outputTgtLocation)
  }
}