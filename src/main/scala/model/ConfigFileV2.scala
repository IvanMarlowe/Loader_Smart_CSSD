package model

class ConfigFileV2 extends ConfigFile {
  protected var _listFile: String = _
  protected var _dumpLocation: String = _
  
  def listFile: String = _listFile
  def listFile_(lf: String) = _listFile = lf
  
  def dumpLocation: String = _dumpLocation
  
  def dumpLocation_(dl: String) = _dumpLocation = dl
  
  
  
}