package helper
import model.ConfigFileV3
import model.Argument
class ConfigBuilderV2(loc: String) extends ConfigBuilder(loc){
//  private var _location = loc
//  
//  override def location() = _location
//  def location_(loc:String) = _location = loc
// 
//  override def generateConfigFile(): ConfigFileV3 = {
//    val data = DataFrameHelper.readJSONFile(location).collect().apply(0)
//    val listFileLocation = ConfigHelper.generateListFileLocation(data)
//    val outputFormat = ConfigHelper.generateOutputFormat(data)
//    val transformList = ConfigHelper.generateTransformProperties(data)
//    val sourceList = ConfigHelper.generateSource
//    val configFile = new ConfigFileV3
//    configFile.listFileLocation_(listFileLocation)
//    configFile.outputFormat_(outputFormat)
//
//    configFile.transformList_(transformList)
//    configFile
//  }
}