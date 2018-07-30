package model

class ConfigFile {
  protected var _location:String = _
  protected var _mappingList:List[ColumnMapping] = _
  protected var _tableList: TableList = _
  protected var _selectQuery: String = _
  protected var _schemaLocation: String = _
  protected var _dataLocation: String = _
  
  def location() = _location
  def mappingList() = _mappingList
  def tableList() = _tableList
  def selectQuery() = _selectQuery
  def schemaLocation() = _schemaLocation
  def dataLocation() = _dataLocation
  
  def location_(loc: String) = _location = loc
  def mappingList_(ml: List[ColumnMapping]) = _mappingList = ml
  def tableList_(tl: TableList) = _tableList = tl
  def selectQuery_(sq: String) = _selectQuery = sq
  def schemaLocation_(sl: String) = _schemaLocation = sl
  def dataLocation_(dl: String) = _dataLocation = dl
}