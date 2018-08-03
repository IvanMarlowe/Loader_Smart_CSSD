package helper
import org.apache.hadoop.fs.Path
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
  def getRecordCount = recordAccumulator.value
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
        println(recordAccumulator + " RA")
      }
     
  }
  def resetCount = recordAccumulator.setValue(0)
  
  def addLogs(transform: Transform){
    val processNumber = transform.processNumber
    val locationDump = transform.targetLocation
    val totalRecordStr = "Total Record Count persisted at location '" + locationDump + "': " + OutputLogger.getRecordCount + " Records."
    collectedLogs = collectedLogs:::List(totalRecordStr)
  }
  
  def generateLogs(configFile: ConfigFileV3){
    val location = configFile.logLocation
    val fs = FileSystem.get(ContextHelper.getSparkContext().hadoopConfiguration)
    if(fs.exists(new Path(location))){
      fs.delete(new Path(location),true)
    }
    
    ContextHelper.getSparkContext().parallelize(collectedLogs.toSeq).saveAsTextFile(configFile.logLocation)
  }
}