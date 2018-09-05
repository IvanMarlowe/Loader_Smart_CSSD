package helper
import org.apache.spark.sql.Row
import org.apache.hadoop.fs._
import sys.process._
import model.URIParam
import model.Argument
import model.{Transform, SourceInfo}
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions._
import model.ConfigFileV3
object ConfigHelper {   
  def generateEnvLocation(data: Row) = {
    data.getAs[String]("env_location")
  }
  
  def generateLogLocation(data: Row) = {
    data.getAs[String]("log_location")
  }
  
  def generateIsCached(data: Row) = {
    data.getAs[String]("cached").toLowerCase().toBoolean
  }
  
  def generateSource(data: Row) = {
    data.getAs[String]("source")
  }
  
  def generateMetadata(data: Row) = {
    data.getAs[String]("metadata")
  }
  
  def generateFileDelimiter(data:Row) = {
    data.getAs[String]("file_delimiter")
  }
  
  def generateFileType(data:Row) = {
    data.getAs[String]("file_type")
  }
  
  def generateSourceType(data:Row) = {
    data.getAs[String]("source_type")
  }
 
  def generatePartitionSize(data:Row) = {
    data.getAs[String]("partition_size").toInt
  }
  
  def generateShuffled(data:Row) = {
    data.getAs[String]("shuffled")
  }
  
  def generateTarget(data:Row) = {
    data.getAs[String]("target")
  }
  
  def generateTargetType(data:Row) = {
    data.getAs[String]("target_type")
  }
  
   def generateCached(data:Row) = {
    data.getAs[String]("cached").toBoolean
  }
  
  def generateTruncateTarget(data: Row) = {
    data.getAs[String]("truncate_target").toBoolean
  }
   
  def generatePropertyList(data:Row) = {
    data.getAs[WrappedArray[String]]("set_properties").toList
  }
  
  def generateName(data:Row) = {
    data.getAs[String]("name")
  }
  
  def generateHql(data: Row) = {
    data.getAs[String]("hql")
  }
  
  def generatePartitionColumns(data: Row) = {
    data.getAs[WrappedArray[String]]("partition_columns").toList
  }
  
  def generateSourceProperties(data:Row): List[SourceInfo] = {
    val rowData = data.getAs[WrappedArray[Row]]("source_list")
    
    val collected = rowData.map(sourceData => {
      val source = generateSource(sourceData)
      val metadata = generateMetadata(sourceData)
      val fileDelimiter = generateFileDelimiter(sourceData)
      val fileType = generateFileType(sourceData)
      val sourceType = generateSourceType(sourceData)
      val partitionSize = generatePartitionSize(sourceData)
      val shuffled = generateShuffled(sourceData)
      val isCached = generateIsCached(sourceData)
      val name = generateName(sourceData)
      val sourceInfo = new SourceInfo
      sourceInfo.name_(name)
      sourceInfo.source_(source)
      sourceInfo.metadata_(metadata)
      sourceInfo.fileDelimiter_(fileDelimiter)
      sourceInfo.fileType_(fileType)
      sourceInfo.sourceType_(sourceType)
      sourceInfo.partitionSize_(partitionSize)
      sourceInfo.shuffled_(shuffled)
      sourceInfo.cached_(isCached)
      sourceInfo
    }).toList
    collected
  }
  
  
  
  def generateTransformProperties(data:Row) : List[Transform] = {
    val rowData = data.getAs[WrappedArray[Row]]("transform_list")
    rowData.map(transformData => {
      val target = generateTarget(transformData)
      val targetType = generateTargetType(transformData)
      val cached = generateCached(transformData)
      val setPropertyList = generatePropertyList(transformData)
      val hql = generateHql(transformData)
      val partitionColumns = generatePartitionColumns(transformData)
      val fileType = generateFileType(transformData)
      val shuffled = generateShuffled(transformData)
      val truncateTarget = generateTruncateTarget(transformData)
      val partitionSize = generatePartitionSize(transformData)
      val sourceList = generateSourceProperties(transformData)
      val transformInfo = new Transform
      transformInfo.targetType_(targetType)
      transformInfo.truncateTarget_(truncateTarget)
      transformInfo.target_(target)
      transformInfo.cached_(cached)
      transformInfo.setProperties_(setPropertyList)
      transformInfo.hql_(hql)
      transformInfo.partitionColumns_(partitionColumns)
      transformInfo.fileType_(fileType)
      transformInfo.shuffled_(shuffled)
      transformInfo.partitionSize_(partitionSize)
      transformInfo.sourceList_(sourceList)
      transformInfo
    }).toList
  }
  
  def iterateSource(transform: Transform, delimiter: String){
//    val sourceList = transform.sourceList
//    sourceList.map(source => {
//      val sourceType = source.typing
//      val dataLocation = source.dataLocation
//      val schemaLocation = source.schemaLocation
//      if(sourceType.equalsIgnoreCase("SEQUENCE")){
//        DataFrameHelper.registerAllTables(configFile, dataLocation, schemaLocation, source, delimiter)
//      }
//      else if(sourceType.equalsIgnoreCase("CSV")){
//        DataFrameHelper.registerAllTables(dataLocation, schemaLocation, source)
//      }
//    })  
  }
  
//  def generateListFileAsSource(configFile: ConfigFileV3, args: Argument, uriParam: URIParam){
//    val delimiter = args.delimiter()
//    val cdrType = uriParam.cdrType
//    val listFileLocation = uriParam.listFileLocation
//    val schemaLocation = uriParam.schemaLocation
//    val tempTableName = uriParam.tempTableName
//    val isCached = uriParam.isCached
//    
//    DataFrameHelper.generateDataFrameFromListFile(tempTableName, configFile, listFileLocation, schemaLocation, delimiter, cdrType, isCached)
//  }
  
  def iterateTransformation(configFile: ConfigFileV3){
//    
    val hiveContext = ContextHelper.getHiveContext
    val sparkContext = ContextHelper.getSparkContext
    val transformList = configFile.transformList
    
    transformList.map(transform => {
      val setPropertiesList = transform.setProperties
      setPropertiesList.map(property => {
        hiveContext.sql(property)
      })
      
      val sourceList = transform.sourceList
      sourceList.map(source => {
        val shuffled = source.shuffled
        val schemaLocation = source.metadata
        val fileDelimiter = source.fileDelimiter
        val sourceType = source.sourceType
        val sourceFileOrName = source.source
        val cached = source.cached
        val partitionSize = source.partitionSize
        val fileType = source.fileType
        val tblName = source.name
        
        if(cached.equals(true)){
          if(fileType.equalsIgnoreCase("csv")){
            if(sourceType.equalsIgnoreCase("file")){
              val schema = DataFrameHelper.generateSchemaFromAvro(schemaLocation)
              DataFrameHelper.readCSVFileWithCustomSchema(sourceFileOrName, schema, fileDelimiter).registerTempTable(tblName)
            }
            else{
              DataFrameHelper.generateDataFrameFromListFileSequence(configFile, source)
            }
          }
          else if(fileType.equalsIgnoreCase("parquet")){
            if(sourceType.equalsIgnoreCase("file")){
              if(shuffled.equals(true)){
                hiveContext.read.parquet(sourceFileOrName).repartition(partitionSize).cache.registerTempTable(tblName)
              }
              else{
                hiveContext.read.parquet(sourceFileOrName).coalesce(partitionSize).cache.registerTempTable(tblName)
              }
            }
            else{
              DataFrameHelper.generateDataFrameFromListFileSequence(configFile, source)
            }
          }
          else{
            //add avro in the future
          }
        }
        //Not Cached
        //To be optimized
        else{
          if(fileType.equalsIgnoreCase("csv")){
            if(sourceType.equalsIgnoreCase("file")){
              val schema = DataFrameHelper.generateSchemaFromAvro(schemaLocation)
              DataFrameHelper.readCSVFileWithCustomSchema(sourceFileOrName, schema, fileDelimiter).registerTempTable(tblName)
            }
            else{
              DataFrameHelper.generateDataFrameFromListFileSequence(configFile, source)
            }
          }
          else if(fileType.equalsIgnoreCase("parquet")){
            if(sourceType.equalsIgnoreCase("file")){
              if(shuffled.equals(true)){
                hiveContext.read.parquet(sourceFileOrName).repartition(partitionSize).registerTempTable(tblName)
              }
              else{
                hiveContext.read.parquet(sourceFileOrName).coalesce(partitionSize).registerTempTable(tblName)
              }
            }
            else{
              DataFrameHelper.generateDataFrameFromListFileSequence(configFile, source)
            }
          }
          else{
            //add avro in the future
          }
        }
      })
      
      val hql = transform.hql
      val target = transform.target
      val targetType = transform.targetType
      val cached = transform.cached
      val partitionSize = transform.partitionSize
      val shuffled = transform.shuffled
      val partitionColumns = transform.partitionColumns
      val truncateTarget = transform.truncateTarget
      val fileType = transform.fileType
      val truncateMode = truncateTarget match {
        case true => "overwrite"
        case false => "append"
      }
      
      if(targetType.equalsIgnoreCase("dataframe")){
        if(cached.equals(false) && shuffled.equals(true)){
          ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).registerTempTable(target)
        }
        else if(cached.equals(false) && shuffled.equals(false)){
          ContextHelper.getHiveContext.sql(hql).registerTempTable(target)
        }
        else if(cached.equals(true) && shuffled.equals(true)){
          ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).cache().registerTempTable(target)
        }
        else{
          ContextHelper.getHiveContext.sql(hql).cache.registerTempTable(target)
        }
      }
      else if(targetType.equalsIgnoreCase("directory")){
        if(cached.equals(false) && shuffled.equals(true)){
          if(fileType.equalsIgnoreCase("parquet")){
            ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).write.mode(truncateMode).partitionBy(partitionColumns:_*).parquet(target)
          }
          else if(fileType.equalsIgnoreCase("csv")){
            val df = ContextHelper.getHiveContext.sql(hql).repartition(partitionSize)
            PersistHelper.saveCSVFile(configFile, transform, df)
          }
        }
        else if(cached.equals(false) && shuffled.equals(false)){
          if(fileType.equalsIgnoreCase("parquet")){
            ContextHelper.getHiveContext.sql(hql).coalesce(partitionSize).write.mode(truncateMode).partitionBy(partitionColumns:_*).parquet(target)
          }
          else if(fileType.equalsIgnoreCase("csv")){
            val df = ContextHelper.getHiveContext.sql(hql).coalesce(partitionSize)
            PersistHelper.saveCSVFile(configFile, transform, df)
          }
        }
        else if(cached.equals(true) && shuffled.equals(true)){
          if(fileType.equalsIgnoreCase("parquet")){
            ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).cache().write.mode(truncateMode).partitionBy(partitionColumns:_*).parquet(target)
          }
          else if(fileType.equalsIgnoreCase("csv")){
            val df = ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).cache()
            PersistHelper.saveCSVFile(configFile, transform, df)
          }
        }
        else{
          if(fileType.equalsIgnoreCase("parquet")){
            ContextHelper
            .getHiveContext
            .sql(hql)
            .coalesce(partitionSize)
            .cache
            .write
            .mode(truncateMode)
            .partitionBy(partitionColumns:_*)
            .parquet(target)
          }
          else if(fileType.equalsIgnoreCase("csv")){
            val df = ContextHelper.getHiveContext.sql(hql).coalesce(partitionSize).cache()
            PersistHelper.saveCSVFile(configFile, transform, df)
          }
        }
      }
      else if(targetType.equalsIgnoreCase("table")){
        if(cached.equals(false) && shuffled.equals(shuffled)){
          ContextHelper.getHiveContext.sql(hql).repartition(partitionSize).write.mode(truncateMode).partitionBy(partitionColumns:_*).saveAsTable(target)
        }
        else if(cached.equals(false) && shuffled.equals(false)){
          ContextHelper.getHiveContext.sql(hql).write.mode(truncateMode).partitionBy(partitionColumns:_*).saveAsTable(target)
        }
        else if(cached.equals(true) && shuffled.equals(true)){
          ContextHelper
          .getHiveContext
          .sql(hql)
          .repartition(partitionSize)
          .cache()
          .write.mode(truncateMode).partitionBy(partitionColumns:_*).saveAsTable(target)
        }
        else{
          ContextHelper.getHiveContext.sql(hql).cache.write.mode(truncateMode).partitionBy(partitionColumns:_*).saveAsTable(target)
        }
      }
    })
  }
}