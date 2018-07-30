package helper
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import model.ColumnType
import model.ColumnType._
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.io.{IntWritable, Text}
import scala.util.{Try, Success, Failure}
import model.ColumnMapping
import model.ConfigFile
import model.ArgumentGlobal
object DataFrameHelper {
  private val outputLogger = new OutputLogger
  
  def getOutputCount = outputLogger.getRecordCount
  
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
    val schema = StructType(
      schemaList.map(data => {
        val dataType = data.getAs[WrappedArray[String]](typeCol).toList
        StructField(
          data.getAs[String](nameCol), 
          DataManipulator.matchDataTypeInAvro(dataType.apply(1)), 
          true
        )
      }):::List(StructField(fileNameCol, org.apache.spark.sql.types.StringType, true))
      
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
     csvDF.repartition(10).cache()
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
      case Success(data) => data.schema(schema).load(location).cache()
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
  
  def registerAllTables(listFileLocation: String, schemaLocation: String, delimiter: String, configFile: ConfigFile, cdrType: String) {
    
    generateDataFrameFromListFile(listFileLocation, schemaLocation, delimiter, cdrType)
    .registerTempTable(configFile.tableList().baseTableInfo.name())
  }
  
  def generateOutput(fileName: String, configFile: ConfigFile) = {
     ContextHelper
    .getHiveContext.sql(configFile.selectQuery()).repartition(1).write.mode("append").parquet(fileName)
  }
  
  def readListFile(loc: String, cdrType: String):List[String] = {
    val returned = ContextHelper.getSparkContext().textFile(loc).collect().toList.filter(seqFileLoc => DataManipulator.chosenCdr(cdrType, seqFileLoc).equals(true))
    returned
  }
  
  def generateRDDRows(seqFiles:List[String], delimiter: String): RDD[Row] = {
    val seqList = seqFiles.flatMap(seqFileLoc => {
         val seq = ContextHelper.getSparkContext.sequenceFile(seqFileLoc, classOf[IntWritable], classOf[Text], DataManipulator.getTotalCoresTask.toInt)
         .map(data => data._2.toString())
         seq.collect
   })
   
   
   
   generateCSVList(seqList, delimiter)
   

  }
  
  def generateCSVList(strList: List[String], delimiter: String): RDD[Row] = {
   
   val rowList = strList.map(data => {
     val splitData = data.split(delimiter)
     outputLogger.incrementRecord
     Row(splitData:_*)
   })
   
   ContextHelper.getSparkContext.parallelize(rowList, DataManipulator.getTotalCoresTask.toInt)
  }
  
  def generateDataFrameFromListFile(listFile: String, schemaLoc: String, delimiter: String, cdrType: String): DataFrame = {
    val filteredSequences = readListFile(listFile, cdrType)
    val rdd = generateRDDRows(filteredSequences, delimiter)
    val schema = generateSchemaFromAvro(schemaLoc)
    broadcast(ContextHelper.getHiveContext.createDataFrame(rdd, schema))
  }
  
}

