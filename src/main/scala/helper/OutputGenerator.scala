package helper
import org.apache.spark.sql.types._
import model.ConfigFileV2
import org.apache.spark.sql.functions._
import model.Argument
import model.ConfigFile
import model.Argument
class OutputGenerator(args: Argument){
  
  private val JSONFile = args.jsonFile()
  private val cdrType = args.cdrType()
  private val delimiter = ","
  private val configBuilder = new ConfigBuilderV3(JSONFile)
  private val configFile = configBuilder.generateConfigFile()//build config file based on JSON File Location Specified
  

  def ingestFile = {
    /*Register the following table:
     * Base Table from the CSV Data and Avro Schema
     * List of Decode Tables from CSV
     * */
    ConfigHelper.iterateSource(configFile, cdrType, delimiter)
    ConfigHelper.iterateTransformation(configFile)
    //Generate Record count and save as log file
    OutputLogger.generateLogs(configFile)

  }
}