package helper
import model.ConfigFileV2
import model.Argument
class ConfigBuilderV2(loc: String) extends ConfigBuilder(loc){
//  private var _location = loc
//  
//  override def location() = _location
//  def location_(loc:String) = _location = loc
 
  override def generateConfigFile(): ConfigFileV2 = {
    val data = DataFrameHelper.readJSONFile(location).collect().apply(0)
//    val additionalMappingList = DataManipulator.generateMappingClassFromJSON(data, "add_map_list")
    val selectQuery = DataManipulator.generateSelectQuery(data, "hql")
    val schemaLocation = DataManipulator.generateSchemaLocation(data, "schema_loc")
    val dataLocation = DataManipulator.generateDataLocation(data, "data_loc")
    val tableList = DataManipulator.generateTableList(data, "tbl_list")
    val dumpLocation = DataManipulator.generateDumpLocation(data, "dmp_loc")
    val listFile = DataManipulator.generateListFile(data, "list_file_loc")
    val configFile = new ConfigFileV2()
    configFile.location_(location)
    configFile.dumpLocation_(dumpLocation)
    configFile.tableList_(tableList)
//    configFile.mappingList_(additionalMappingList)
    configFile.selectQuery_(selectQuery)
    configFile.schemaLocation_(schemaLocation)
    configFile.dataLocation_(dataLocation)
    configFile.listFile_(listFile)
    configFile
  }
}