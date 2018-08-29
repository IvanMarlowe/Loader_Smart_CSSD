package helper
import org.apache.hadoop.fs.Path
import sys.process._
import org.apache.spark.sql.DataFrame
import org.json4s.jackson.JsonMethods
import scala.io.Source.fromURL
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import model.{Transform, ConfigFileV3}
import scala.util.{Success, Failure, Try}
object OutputLogger {
  
  private var recordAccumulator = ContextHelper.getSparkContext.accumulator(0, "Record Count")
  private var stageAccumulator = ContextHelper.getSparkContext().accumulator(0, "Stage Id")
  private var collectedLogs = List[String]()
  private var runTime = System.nanoTime()
  def getRecordCount = 0//recordAccumulator.value
  
  @deprecated
  def incrementRecord(taskEnd:SparkListenerTaskEnd) = {

      if(taskEnd.taskInfo.accumulables.size > 0 && taskEnd.taskType.toLowerCase().contains("shuffle")){
        val extractedValue = taskEnd.taskMetrics.shuffleWriteMetrics.get.shuffleRecordsWritten.toInt
        if(stageAccumulator.value == taskEnd.stageId.toInt){
          recordAccumulator += extractedValue
        }
        else{
          stageAccumulator.setValue(taskEnd.stageId.toInt)
          recordAccumulator.setValue(extractedValue)
        }
      }
     
  }
  
  def resetCount = 0//recordAccumulator.setValue(0)
  
  def addRecordSizeToLogs(transform: Transform, count: Int){
    val processNumber = transform.processNumber
    val locationDump = transform.targetLocation
    val totalRecordStr = "Total Record Count persisted at location '" + locationDump + "': " + count + " Records."
    collectedLogs = collectedLogs:::List(totalRecordStr)
  }
  
  def addRecordSizeInBytesToLogs(transform: Transform, tmpLocation: String){
    val locationDump = transform.targetLocation
    val strBytes = ("hdfs dfs -du -s " + tmpLocation).!!.toString().dropRight(1).split(" ").apply(0)
    val sizeStr = "Total size persisted at location '" + locationDump + "': " + strBytes + " bytes."
    collectedLogs = collectedLogs:::List(sizeStr)
  }
  
  def generateLogs(configFile: ConfigFileV3){
    
    DataManipulator.deleteIfExistsFile(configFile.logLocation)
    val logs = collectedLogs.toSeq:+("Total Transformation Run time: " + (System.nanoTime - runTime) / 1e9d)
    ContextHelper.getSparkContext().parallelize(logs, 1).saveAsTextFile(configFile.logLocation)
  }
}