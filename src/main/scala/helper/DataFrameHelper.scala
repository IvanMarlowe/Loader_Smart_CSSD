package helper
import org.apache.spark.sql.types._
import model.SourceInfo
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import model.ColumnType
import model.ColumnType._
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.io.{IntWritable, Text, BytesWritable}
import scala.util.{Try, Success, Failure}
import model.ColumnMapping
import model.{ConfigFile, ConfigFileV3}
import model.ArgumentGlobal
import java.nio.charset.StandardCharsets
import implicits.HiveContextUtils._
object DataFrameHelper {

  def generateSchemaFromAvro(location: String): StructType = {
    val fieldColName = "fields"
    val nameCol = "name"
    val typeCol = "type"
    val fileNameCol = "file_name"
    val schemaFile = readAvro(location)
    val schemaList = schemaFile.select(fieldColName)
    .collect()
    .apply(0)
    .apply(0)
    .asInstanceOf[WrappedArray[Row]]
    .toList
    
    val distinctList = schemaList
    .map(data => data.getAs[String](nameCol))
    .groupBy(identity)
    .collect { case (x,ys) if ys.lengthCompare(1) > 0 => x }
    
    if(distinctList.size > 1){
      CleanupHelper.cleanup()
      throw new Exception("Duplicate Column Detected: " + distinctList.mkString(", "))
    }
    
    val schema = StructType(
      schemaList.map(data => {
        val dataType = data.getAs[WrappedArray[String]](typeCol).toList
        StructField(
          data.getAs[String](nameCol), 
          DataManipulator.matchDataTypeInAvro(dataType.apply(1)), 
          true
        )
      })//:::List(StructField(fileNameCol, org.apache.spark.sql.types.StringType, true))
      
    )
    schema
  }
  
  def generateDataFrameFromCSV(CSVLocation: String, avroLocation:String, delimiter: String): DataFrame = {
    
    val fileNameCol = "file_name"
    val schema = generateSchemaFromAvro(avroLocation)
    val csvDF = readCSVFileWithCustomSchema(CSVLocation, schema, delimiter)
    .withColumn(fileNameCol, lit(CSVLocation)
        .cast(org.apache.spark.sql.types.StringType)
     )
     csvDF.repartition(DataManipulator.getTotalCoresTask() * 2).cache()
  }
  
    def readAvro(location: String): DataFrame = {
    Try(ContextHelper.getHiveContext.read.format("com.databricks.spark.avro").json(location)) match {
      case Success(data) => data
      case Failure(data) => throw new Exception("Avro File at location '" + location + "' does not exist.")
    }
  }
  
  def readCSVFile(location:String, delimiter: String, isHeader: String): DataFrame = {
     Try(
        ContextHelper.getHiveContext.read.format("com.databricks.spark.csv")
        .option("header", isHeader)
        .option("inferSchema", "false")
        .option("delimiter", delimiter)
      ) match {
      case Success(data) => data.load(location).cache()
      case Failure(data) => throw new Exception("CSV File at location '" + location + "' does not exist.")
    }
  }
  
  def readJSONFile(location: String): DataFrame = {
    ContextHelper.getHiveContext.read.json(location)
  }
  
  def readCSVFileWithCustomSchema(location:String, schema: StructType, delimiter: String): DataFrame = {
     Try(
        ContextHelper.getHiveContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", delimiter)
      ) match {
      case Success(data) => data.schema(schema).load(location)
      case Failure(data) => throw new Exception("CSV File at location '" + location + "' does not exist.")
     }
  }
  
  def readCSVFileWithCustomSchema(location:String, schema: StructType, delimiter: String, isCached: Boolean): DataFrame = {
       Try(
          ContextHelper.getHiveContext.read.format("com.databricks.spark.csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", delimiter)
        ) match {
        case Success(data) => {
          val df = data.schema(schema).load(location)  
          if(isCached.equals(true)){
            df.cache()
          }
          else{
            df
          }
        }
        case Failure(data) => throw new Exception("CSV File at location '" + location + "' does not exist.")
       }
  }
  
  def generateWithAdditionalColumn(columnMappingList: List[ColumnMapping], df: DataFrame): DataFrame = {
    val schemaDf = df.schema.map(data => data.name.toString()).toList
    val additionalMappingQuery = columnMappingList.map(data => data.mapping() + " " + data.name()).toList
    val resultingSelectExp = schemaDf:::additionalMappingQuery
    df.selectExpr(resultingSelectExp:_*)
  }
  
  def newDfAddCol(df: DataFrame, configFile: ConfigFile): DataFrame = {
     generateWithAdditionalColumn(configFile.mappingList(), df)
  }
  
  def registerAdditionalTables(delimiter: String, configFile: ConfigFile) = {
    val tableList = configFile.tableList().sourceTableInfoList
    val delimiter = ArgumentGlobal.delimiter()
    val seqLocList = tableList.map(data => {
      broadcast(DataFrameHelper.readCSVFile(data.location, delimiter, "true").cache()).registerTempTable(data.name)
      data.location
    })
    
    
  }
  
  @deprecated
  def registerAllTables(configFile: ConfigFileV3, listFileLocation: String, schemaLocation: String, delimiter: String, source: SourceInfo, cdrType: String) {
//    generateDataFrameFromListFile(source.name, configFile, listFileLocation, schemaLocation, delimiter, cdrType, true)
  }
  
  //csv location
  def registerAllTables(CSVLocation: String, schemaLocation: String, source: SourceInfo) {
//    generateDataFrameFromCSV(CSVLocation, schemaLocation, ",").registerTempTable(source.name)
  }
  
  def registerAllTables(configFile: ConfigFileV3, sequenceLocation: String, schemaLocation: String, source: SourceInfo, delimiter: String){
//    val tableName = source.name
//    generateDataFrameFromSequenceFile(tableName, configFile, List(sequenceLocation),delimiter, schemaLocation, true).registerTempTable(tableName)
  }
  
  
  def generateOutput(fileName: String, configFile: ConfigFile) = {
     ContextHelper
    .getHiveContext.sql(configFile.selectQuery()).repartition(DataManipulator.getTotalCoresTask()).write.mode("append").parquet(fileName)
  }
  
  def readListFile(loc: String):List[String] = {
    val returned = ContextHelper.getSparkContext().textFile(loc).collect().toList
    returned
  }
  
  @deprecated
  def generateRDDRows(seqFiles:List[String], delimiter: String, schema: StructType): RDD[Row] = {
    val seqList = seqFiles.flatMap(seqFileLoc => {
      val seq = ContextHelper.getSparkContext.sequenceFile(seqFileLoc, classOf[IntWritable], classOf[BytesWritable], DataManipulator.getTotalCoresTask)
      .flatMap(data => new String(data._2.copyBytes(), StandardCharsets.UTF_8).split("\n"))
      seq.collect()
    })
    
    

   generateCSVList(seqList, delimiter, schema)
  }
  
  @deprecated
  def generateCSVList(strList: List[String], delimiter: String, schema: StructType): RDD[Row] = {
   val schemaSize = schema.size
   val dataSize = strList.apply(0).split("\\" + delimiter, -1).size
   val reducedSize = (dataSize - schemaSize).abs
   val rowList = strList.map(data => {
     val splitData = data.split(DataManipulator.replaceStringEscaped(delimiter), -1).dropRight(reducedSize)
     Row(splitData:_*)
   })

   ContextHelper.getSparkContext.parallelize(rowList, DataManipulator.getTotalCoresTask() * 2)
  }
  
  def generateDataFrameFromListFileSequence(configFile: ConfigFileV3, source: SourceInfo){
    val listFile = source.source
    val tblName = source.name
    val seqFiles = readListFile(listFile)
    val fileType = source.fileType
    generateDataFrameFromSequenceFile(configFile, seqFiles, source)
    .registerTempTable(tblName)
  }
  
  
  def generateDataFrameFromSequenceFile(configFile: ConfigFileV3, seqFiles: List[String], source: SourceInfo): DataFrame = {
    val schemaLoc = source.metadata
    val delimiter = source.fileDelimiter
    val isCached = source.cached
    val tblName = source.name
    val partitionSize = source.partitionSize
    
    val fileType = source.fileType
    val location = configFile.envLocation.concat("/" + ContextHelper.getSparkContext().applicationId).concat("_").concat(tblName)
    if(fileType.equalsIgnoreCase("csv")){
      val schema = generateSchemaFromAvro(schemaLoc)
      ContextHelper.getHiveContext.saveTemporaryCSV(seqFiles, location)
      readCSVFileWithCustomSchema(location, schema, delimiter, isCached)
    }
    else{
      ContextHelper.getHiveContext.createDataFrameFromListFileParquet(source, seqFiles)
    }
  }
  
  def generateDataFrameFromSequence(schema: StructType, rdd: RDD[Row], isCached: Boolean): DataFrame = {
    if(isCached.equals(true)){  
      ContextHelper.getHiveContext.createDataFrame(rdd, schema).cache()
    }
    else{
      ContextHelper.getHiveContext.createDataFrame(rdd, schema)
    }
  }
  
  def saveTemporaryCSV(seqFiles: List[String], location: String){
    val emptyRDD:RDD[String] = ContextHelper.getSparkContext().emptyRDD
    val sequenceList = seqFiles.map(seqLoc => {
      val sequenceList = ContextHelper.getSparkContext().sequenceFile(seqLoc, classOf[IntWritable], classOf[BytesWritable], DataManipulator.getTotalCoresTask)
      sequenceList
      .flatMap(data => new String(data._2.copyBytes(), StandardCharsets.UTF_8).split("\n"))
      .repartition(DataManipulator.getTotalCoresTask)
    })
    .foldLeft(emptyRDD)((x, y) => x.union(y))

    DataManipulator.deleteIfExistsFile(location)
    CleanupHelper.addToDelete(location)
    sequenceList.saveAsTextFile(location)
  }
}

