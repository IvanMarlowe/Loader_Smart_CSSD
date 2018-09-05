package model

class ConfigFileV3 extends ConfigFile{
  private var _transformList: List[Transform] = List[Transform]()
  private var _logLocation = ""
  private var _envLocation = ""
  
  def envLocation = _envLocation
  def logLocation = _logLocation
  def transformList = _transformList
  
  def envLocation_(el: String) = _envLocation = el
  def logLocation_(ll: String) = _logLocation = ll
  def transformList_(tl: List[Transform]) = _transformList = tl
}