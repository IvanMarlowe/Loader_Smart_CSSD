package model

class ConfigFileV3 extends ConfigFile{
  private var _transformList: List[Transform] = List[Transform]()
  private var _sourceList = List[SourceInfo]()
  private var _logLocation = ""
  def logLocation = _logLocation
  def transformList = _transformList
  def sourceList = _sourceList
  def logLocation_(ll: String) = _logLocation = ll
  def sourceList_(sl: List[SourceInfo]) = _sourceList = sl
  def transformList_(tl: List[Transform]) = _transformList = tl
}