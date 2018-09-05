package implicits
import helper.{DataManipulator, CleanupHelper, ContextHelper}
import model.SourceInfo
import org.apache.hadoop.io.{IntWritable, Text, BytesWritable}
import org.apache.spark.rdd.RDD
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
object HiveContextUtils {
  implicit class HiveWithSaveCSV(x: HiveContext) {
    def saveTemporaryCSV(seqFiles: List[String], location: String) = {
      val emptyRDD:RDD[String] = ContextHelper.getSparkContext().emptyRDD
      val sequenceList = seqFiles.map(seqLoc => {
        val sequenceList = ContextHelper.getSparkContext().sequenceFile(seqLoc, classOf[IntWritable], classOf[BytesWritable], DataManipulator.getTotalCoresTask * 2)
        sequenceList
        .map(data => {
          new String(data._2.copyBytes(), StandardCharsets.UTF_8)
        })
      })
      .foldLeft(emptyRDD)((x, y) => x.union(y))
  
      DataManipulator.deleteIfExistsFile(location)
      CleanupHelper.addToDelete(location)
      sequenceList.repartition(DataManipulator.getTotalCoresTask).saveAsTextFile(location)
    }
    
    def createDataFrameFromListFileParquet(source: SourceInfo, seqFiles: List[String]): DataFrame = {
      val isCached = source.cached
      val isShuffled = source.shuffled
      val partitionSize = source.partitionSize
      
      val df = if(isShuffled.equals(true)){
        ContextHelper.getHiveContext.read.parquet(seqFiles:_*).repartition(partitionSize)
      }
      else{
        ContextHelper.getHiveContext.read.parquet(seqFiles:_*).coalesce(partitionSize)
      }
      
      if(isCached){
        df.cache()
      }
      else{
        df
      }
    }
    
    def createDataFrameFromListFileCSV(schema: StructType, source: SourceInfo, seqFiles: List[String]): DataFrame = {
      val emptyRDD:RDD[Row] = ContextHelper.getSparkContext().emptyRDD
      val delimiter = source.fileDelimiter
      val isCached = source.cached
      val rowRDD = seqFiles.map(seqLoc => {
        val strRDD = ContextHelper.getSparkContext()
        .sequenceFile(seqLoc, classOf[IntWritable], classOf[BytesWritable], DataManipulator.getTotalCoresTask)
        .map(data => {
          new String(data._2.copyBytes(), StandardCharsets.UTF_8)
          .split("\n")
          .map(strRows => {
            strRows.split("\\".concat(delimiter)).dropRight(2)
          })
        })
        .map(strList => Row(strList:_*))
        strRDD
      })
      .foldLeft(emptyRDD)((x, y) => x.union(y))
      .repartition(DataManipulator.getTotalCoresTask())
      .cache()
      
      
      val df = ContextHelper.getHiveContext.createDataFrame(rowRDD, schema)
      
      if(isCached.equals(true)){  
        df.cache()
      }
      else{
        df
      }
    }
  }
}