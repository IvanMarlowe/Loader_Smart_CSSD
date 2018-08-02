package helper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import model.{Transform, ConfigFileV3}
import scala.util.{Success, Failure, Try}
object OutputLogger {
  private var recordAccumulator = ContextHelper.getSparkContext.accumulator(0, "Record Count")
  private var collectedLogs = List[String]()
  def getRecordCount = recordAccumulator
  def incrementRecord(taskEnd:SparkListenerTaskEnd) = {
    
      if(taskEnd.taskInfo.accumulables.size > 0 && taskEnd.taskType.toLowerCase().contains("shuffle")){
        recordAccumulator += taskEnd.taskMetrics.shuffleWriteMetrics.get.shuffleRecordsWritten.toInt
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