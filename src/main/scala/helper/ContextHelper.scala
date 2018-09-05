package helper
import UDF.Functions
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import scala.util.{Try, Success, Failure}
object ContextHelper {
  
  private var _sparkContext: SparkContext = _
  private var _sparkConf: SparkConf = _
  private var _hiveContext: HiveContext = _
  
  initializeSparkContext
  initializeHiveContext
  
  def getSparkContext(): SparkContext = {
      _sparkContext
  }
  
  private def initializeSparkContext: SparkContext = {
    _sparkContext = new SparkContext(initializeSparkConf)
//    addCountListener
    _sparkContext
  }
  
  private def initializeSparkConf = {
    new org.apache.spark.SparkConf()
//      .setAppName("Smart_ingest")
//      .setMaster("local")
  }
  
  private def initializeHiveContext:HiveContext = {
    _hiveContext = new HiveContext(getSparkContext)
    
    ApplyUDFs(_hiveContext, new Functions(""))
    _hiveContext
  }
  
  def getHiveContext = {
    _hiveContext
  }
  
  private def ApplyUDFs(hiveContext: HiveContext, function: Functions) {
//    hiveContext.udf.register("md5", function.md5Converter)//apply md5 function as custom hive function
//    hiveContext.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    hiveContext.setConf("spark.sql.parquet.compression.codec","snappy") 
    hiveContext.setConf("spark.dynamicAllocation.enabled ", "true")
    hiveContext.udf.register("convert_hash", function.newHashKeyConverter)
    hiveContext.udf.register("gen_now", function.nowGenerator)
    hiveContext.udf.register("randFunc", function.genRand)
    hiveContext.udf.register("decode_double", function.decodeDouble)
    hiveContext.udf.register("combine", function.combine)
    hiveContext.udf.register("compute", function.computeDouble)
  }
  
//  def addCountListener(){
//    _sparkContext.addSparkListener(new SparkListener() { 
//    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
//
//          OutputLogger.incrementRecord(taskEnd) 
//
//      }
//    })
//  }
}