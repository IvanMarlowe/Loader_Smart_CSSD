package helper
import model.ConfigFileV3
import model.Argument
class ConfigBuilderV3(loc: String) extends ConfigBuilder(loc){
 
  override def generateConfigFile(): ConfigFileV3 = {
    val data = DataFrameHelper.readJSONFile(location).collect().apply(0)
    val transformList = ConfigHelper.generateTransformProperties(data)
    val logLocation = ConfigHelper.generateLogLocation(data)
    val envLocation = ConfigHelper.generateEnvLocation(data)
    val configFile = new ConfigFileV3
    configFile.envLocation_(envLocation)
    configFile.logLocation_(logLocation)
    configFile.transformList_(transformList)
    configFile
  }
}