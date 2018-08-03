package model

class Argument {
  var  _delimiter = ""
  var _location = ""
  var _jsonFile = ""
  var _cdrType = ""
  var _fileURI = ""
  
  def delimiter(): String = _delimiter
  def location(): String = _location
  def jsonFile(): String = _jsonFile 
  def cdrType(): String = _cdrType
  def fileURI(): String = _fileURI
  def delimiter_(dm: String): Unit = _delimiter = dm
  def location_(tn: String): Unit = _location = tn
  def jsonFile_(jf: String): Unit = _jsonFile = jf
  def cdrType_(ct: String): Unit = _cdrType = ct
  def fileURI_(fu: String): Unit = _fileURI = fu
  
}