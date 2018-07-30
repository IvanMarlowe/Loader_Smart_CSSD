package helper
import model.ConfigFile
import model.Argument
class ConfigBuilder(loc: String){
  private var _location = loc
  
  def location() = _location
  def location_(loc:String) = _location = loc
  
  def generateConfigFile(): ConfigFile = {
    val data = DataFrameHelper.readJSONFile(location).collect().apply(0)
    val additionalMappingList = DataManipulator.generateMappingClassFromJSON(data, "add_map_list")
    val selectQuery = DataManipulator.generateSelectQuery(data, "hql")
    val schemaLocation = DataManipulator.generateSchemaLocation(data, "schema_loc")
    val dataLocation = DataManipulator.generateDataLocation(data, "data_loc")
    val tableList = DataManipulator.generateTableList(data, "tbl_list")
    val configFile = new ConfigFile()
    configFile.location_(location)
    configFile.tableList_(tableList)
    configFile.mappingList_(additionalMappingList)
    configFile.selectQuery_(selectQuery)
    configFile.schemaLocation_(schemaLocation)
    configFile.dataLocation_(dataLocation)
    configFile
  }
}