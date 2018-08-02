package helper
import model.ConfigFileV3
import model.Argument
class ConfigBuilderV3(loc: String) extends ConfigBuilder(loc){
 
  override def generateConfigFile(): ConfigFileV3 = {
    val data = DataFrameHelper.readJSONFile(location).collect().apply(0)
    val transformList = ConfigHelper.generateTransformProperties(data)
    val sourceList = ConfigHelper.generateSourceProperties(data)
    val logLocation = ConfigHelper.generateLogLocation(data)
    val configFile = new ConfigFileV3
    configFile.logLocation_(logLocation)
    configFile.transformList_(transformList)
    configFile.sourceList_(sourceList)
    configFile
  }
}