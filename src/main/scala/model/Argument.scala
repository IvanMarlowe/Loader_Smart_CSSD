package model

class Argument {
  private var  _delimiter = ""
  private var _location = ""
  private var _jsonFile = ""
//  var _cdrType = ""
  private var _fileURI = ""
  private var _isUnitTest = false
  
  def isUnitTest(): Boolean = _isUnitTest
  def delimiter(): String = _delimiter
  def location(): String = _location
  def jsonFile(): String = _jsonFile 
//  def cdrType(): String = _cdrType
  def fileURI(): String = _fileURI
  
  def delimiter_(dm: String): Unit = _delimiter = dm
  def location_(tn: String): Unit = _location = tn
  def jsonFile_(jf: String): Unit = _jsonFile = jf
//  def cdrType_(ct: String): Unit = _cdrType = ct
  def fileURI_(fu: String): Unit = _fileURI = fu
  def isUnitTest_(iut: Boolean): Unit = _isUnitTest = iut
}