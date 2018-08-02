package helper
import org.apache.spark.sql.Row
import model.{Transform, SourceInfo}
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions._
import model.ConfigFileV3
object ConfigHelper {   
  def generateOutputFormat(data: Row): String = {
    data.getAs[String]("output_format")
  }
  
  def generateSourceFormat(data:Row): String = {
    data.getAs[String]("source_format")
  }
  
  def generateSourceType(data: Row): String = {
    data.getAs[String]("type")
  }
  
  def generateSourceName(data: Row): String = {
    data.getAs[String]("name")
  }
  
  def generateListFileLocation(data:Row): String = {
    data.getAs[String]("list_file_loc")
  }
  
  def generateSchemaLocation(data:Row): String = {
    data.getAs[String]("schema_loc")
  }
  
  def generateProcessNumber(data:Row): String = {
    data.getAs[String]("process_number")
  }
  
  def generateDatabase(data:Row): String = {
    data.getAs[String]("database")
  }
  
  def generateTargetType(data:Row): String = {
    data.getAs[String]("target_type")
  }
  
  def generateTargetName(data:Row): String = {
    data.getAs[String]("target_name")
  }
  
  def generateTruncateTarget(data:Row): String = {
    data.getAs[String]("truncate_target")
  }
  
  def generateLogLocation(data:Row): String = {
    data.getAs[String]("log_location")
  }
  
  def generatePersistToTarget(data:Row): String = {
    data.getAs[String]("persist_to_target")
  }
  
  def generateEndDateSource(data:Row): String = {
    data.getAs[String]("end_date_source")
  }
  
  def generateHql(data:Row): String = {
    data.getAs[String]("hql")
  }
  
  def generateTableName(data:Row): String = {
    data.getAs[String]("table_name")
  }
  
  def generateTargetLocation(data:Row): String = {
    data.getAs[String]("target_location")
  }
  
  def generateSourceLocation(data:Row): String = {
    data.getAs[String]("data_location")
  }
  
  def generateSourceSchemaLocation(data:Row): String = {
    data.getAs[String]("schema_location")
  }
  
  
  def generateSourceProperties(data:Row): List[SourceInfo] = {
    val rowData = data.getAs[WrappedArray[Row]]("source_list")
    
    val collected = rowData.map(sourceData => {
      val dataLocation = generateSourceLocation(sourceData)
      val schemaLocation = generateSourceSchemaLocation(sourceData)
      val typing = generateSourceType(sourceData)
      val name = generateSourceName(sourceData)
      val source = new SourceInfo
      source.dataLocation_(dataLocation)
      source.schemaLocation_(schemaLocation)
      source.typing_(typing)
      source.name_(name)
      source
    }).toList
    collected
  }
  
  
  
  def generateTransformProperties(data:Row) : List[Transform] = {
    val rowData = data.getAs[WrappedArray[Row]]("transform_list")
    rowData.map(transformData => {
      val processNumber = generateProcessNumber(transformData)
      val targetType = generateTargetType(transformData)
      val targetName = generateTargetName(transformData)
      val truncateTarget = generateTruncateTarget(transformData)
      val targetLocation = generateTargetLocation(transformData)
      val outputFormat = generateOutputFormat(transformData)
      val hql = generateHql(transformData)
      val transform = new Transform
      transform.outputFormat_(outputFormat)
      transform.targetType_(targetType)
      transform.targetName_(targetName)
      transform.truncateTarget_(truncateTarget)
      transform.processNumber_(processNumber)
      transform.hql_(hql)
      transform.targetLocation_(targetLocation)
      transform
    }).toList
  }
  
  def iterateSource(configFile: ConfigFileV3, cdrType: String, delimiter: String){
    val sourceList = configFile.sourceList
    sourceList.map(source => {
      val sourceType = source.typing
      val dataLocation = source.dataLocation
      val schemaLocation = source.schemaLocation
      if(sourceType.equalsIgnoreCase("SEQUENCE")){
        DataFrameHelper.registerAllTables(dataLocation, schemaLocation, delimiter, source, cdrType)
      }
      else if(sourceType.equalsIgnoreCase("CSV")){
        DataFrameHelper.registerAllTables(dataLocation, schemaLocation, source)
      }
    })  
  }
  
  def iterateTransformation(configFile: ConfigFileV3){
    
    val hiveContext = ContextHelper.getHiveContext
    val sparkContext = ContextHelper.getSparkContext
    val transformList = configFile.transformList
    transformList.sortWith((t1, t2) => t1.processNumber.toInt < t2.processNumber.toInt).map(transform => {
      
      val hql = transform.hql
      val name = transform.targetName
      val targetType = transform.targetType
      val outputFormat = transform.outputFormat
      val targetLocation = transform.targetLocation
      if(targetType.equalsIgnoreCase("DATAFRAME")){ 
       hiveContext.sql(hql).repartition(DataManipulator.getTotalCoresTask()).registerTempTable(name)
      }
      else if(targetType.equalsIgnoreCase("TABLE")){
         OutputLogger.resetCount
         if(outputFormat.equalsIgnoreCase("PARQUET")){
           hiveContext
           .sql(hql)
           .repartition(DataManipulator.getTotalCoresTask())
           .write.mode("append")
           .parquet(targetLocation)
         }
         else if(outputFormat.equalsIgnoreCase("CSV")){
           hiveContext.sql(hql)
           .repartition(DataManipulator.getTotalCoresTask())
           .write
           .format("com.databricks.spark.csv")
           .mode("overwrite")
           .save(targetLocation)
         }
         else{
           throw new Exception("Only TABLE and CSV Type should be involved.")
         }
         OutputLogger.addLogs(transform)
         
      }
      else{
        throw new Exception("Only TABLE and DATAFRAME Type should be inolved.")
      }
    })
    
  }
}