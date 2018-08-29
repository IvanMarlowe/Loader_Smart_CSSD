package helper
import org.apache.spark.sql.types._
import model.URIParam
import model.ConfigFileV2
import org.apache.spark.sql.functions._
import model.Argument
import model.ConfigFile
import model.Argument
class OutputGenerator(args: Argument){
  
  private val JSONFile = args.jsonFile()
  private val uriParam = new URIParam(args.fileURI())
  private val delimiter = args.delimiter()
  private val configBuilder = new ConfigBuilderV3(JSONFile)
  private val configFile = configBuilder.generateConfigFile()//build config file based on JSON File Location Specified
  

  def ingestFile = {
    /*Register the following table:
     * Base Table from the CSV Data and Avro Schema
     * List of Decode Tables from CSV
     * */
    
    ConfigHelper.generateListFileAsSource(configFile, args, uriParam)
    ConfigHelper.iterateSource(configFile, uriParam, delimiter)
    ConfigHelper.iterateTransformation(configFile)
    //Generate Record count and save as log file
    OutputLogger.generateLogs(configFile)
    CleanupHelper.cleanup()
  }
}