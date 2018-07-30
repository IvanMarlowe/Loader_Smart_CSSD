package helper
import org.apache.spark.sql.types._
import model.ConfigFileV2
import org.apache.spark.sql.functions._
import model.Argument
import model.ConfigFile
import model.Argument
class OutputGenerator(args: Argument){
  
  private val JSONFile = args.jsonFile()
  private val delimiter = args.delimiter()
  private val listFileLocation = args.location()
  private val cdrType = args.cdrType()
  private val configBuilder = new ConfigBuilderV2(JSONFile)
  private val configFile = configBuilder.generateConfigFile()//build config file based on JSON File Location Specified
  private val schemaLocation = configFile.schemaLocation()
  private val dataLocation = configFile.dataLocation()
  private val dumpLocation = configFile.dumpLocation
  

  def ingestFile = {
    /*Register the following table:
     * Base Table from the CSV Data and Avro Schema
     * List of Decode Tables from CSV
     * */
    DataFrameHelper
    .registerAllTables(listFileLocation, schemaLocation, delimiter, configFile, cdrType)
    
    //Generate Output using Query from the JSON File and the View Tables that were registered during the session of Spark
    DataFrameHelper
    .generateOutput(dumpLocation, configFile)
    
    
    println(DataFrameHelper.getOutputCount)
    println(DataFrameHelper)
  }
}