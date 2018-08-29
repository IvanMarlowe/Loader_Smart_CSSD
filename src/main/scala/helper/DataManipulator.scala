package helper
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import model.Transform
import model.SourceTableInfo
import model.TableList
import model.ColumnMapping
import model.ColumnType._
import model.ColumnType
import model.BaseTableInfo
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
object DataManipulator {
  def getTypeReturnable(value: ColumnType): DataType = {
    value match {
      case ColumnType.StringType => org.apache.spark.sql.types.StringType
      case ColumnType.IntType => org.apache.spark.sql.types.IntegerType
    }
  }
  
  def matchDataTypeInAvro(value: String): DataType = {
    val result = value match {
      case "string" => ColumnType.StringType
      case "integer" => ColumnType.IntType
      case _ => ColumnType.StringType
    }
    getTypeReturnable(result)
  }
  
//  def extractColumnMapping(jsonDf: DataFrame): List[ColumnMapping] = {
//    val listName = "list"
//    val partColMap = jsonDf.select(listName).collect().apply(0).apply(0).asInstanceOf[WrappedArray[Row]]
//    partColMap.map(data => new ColumnMapping(data.getAs[String]("name"), data.getAs[String]("mapping"))).toList
//  }
//  
//  def extractCSVTableList(jsonDf: DataFrame): List[String] = {
//    val csvListName = "table_list"
//    val partColMap = jsonDf.selectExpr(csvListName).collect.apply(0).apply(0).asInstanceOf[WrappedArray[String]]
//    partColMap.toList    
//  }
  
  
  def generateListStrFromJSON(rowList: Row, colName: String): List[String] = {
    rowList.getAs[WrappedArray[String]](colName).toList
  } 
  
  def generateMappingClassFromJSON(rowList:Row, colName: String): List[ColumnMapping] = {
    val dataList = rowList.getAs[WrappedArray[Row]](colName).toList
    dataList.map(data => new ColumnMapping(data.getAs[String]("name"), data.getAs[String]("mapping")))
  }
  
//  def generateTableListFromJSON(rowList:Row, colName: String):  List[TableList] =  {
//    val dataList = rowList.getAs[WrappedArray[Row]](colName).toList
//    dataList.map(data => new TableList(data.getAs[String]("name"), data.getAs[String]("location")))
//  }
  
  def generateSelectQuery(rowList: Row, colName: String): String = {
    rowList.getAs[String](colName)
  } 

  def generateBaseTableInfo(data: Row, colName: String): BaseTableInfo = {
    val rowData = data.getAs[Row](colName)
    new BaseTableInfo(rowData.getAs[String]("name"))
  }
  
  def generateSchemaLocation(data: Row, colName: String): String = {
    data.getAs[String]("schema_loc")
  }
  
  def generateDataLocation(data: Row, colName: String): String = {
    data.getAs[String]("data_loc")
  }
  
  def generateSourceTableInfos(data: Row, colName: String): List[SourceTableInfo] = {
//    val rowDataList = data.getAs[WrappedArray[Row]](colName)
//    rowDataList
//    .map(rowData => new SourceTableInfo(rowData.getAs[String]("name"), rowData.getAs[String]("location")))
//    .toList
    null
  }
  
  def generateTableList(data:Row, colName: String): TableList = {
    val rowData = data.getAs[Row](colName)
    val sourceTableInfoList = generateSourceTableInfos(rowData, "src_lst")
    val baseTableInfo = generateBaseTableInfo(rowData, "base_tbl")
    new TableList(baseTableInfo, sourceTableInfoList)
  }
  
  def generateListFile(data:Row, colName: String): String = {
    data.getAs[String]("list_file_loc")
  }
  
  def generateDumpLocation(data:Row, colName: String): String = {
    data.getAs[String]("dmp_loc")
  }
  
  def chosenCdr(cdrTypeVal: String, cdrTypeFileName: String): Boolean = {
    cdrTypeFileName.split("/").last.toLowerCase().split("\\-").apply(3).contains(cdrTypeVal.toLowerCase())
  }
  
  def getTotalCoresTask(): Int = {
    (getExecutorInstanceCount * getExecutorCoreInstanceCount).toInt
  }
  
  def getExecutorInstanceCount(): Int = {
    ContextHelper.getSparkContext().getConf.get("spark.executor.instances").toInt
//    1
   }
   
   def getExecutorCoreInstanceCount(): Int = {
//     4
     ContextHelper.getSparkContext().getConf.get("spark.executor.cores").toInt
   }
   
   def replaceStringEscaped(char: String) = {
     char match{
       case "|" => "|"
       case "," => ","
     }
   }
   
   def deleteIfExistsFile(location: String){
     println("Location to be deleted: " + location)
     val fs = FileSystem.get(ContextHelper.getSparkContext().hadoopConfiguration)
     if(fs.exists(new Path(location))){
      fs.delete(new Path(location),true)
     }
   }

}