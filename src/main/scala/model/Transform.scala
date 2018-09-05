package model

class Transform {
  private var _sourceList = List[SourceInfo]()
  private var _target = ""
  private var _targetType = ""
  private var _setProperties = List[String]()
  private var _hql = ""
  private var _partitionColumns = List[String]()
  private var _fileType = ""
  private var _shuffled = ""
  private var _partitionSize:Int = 0
  private var _cached: Boolean = false
  private var _truncateTarget = false
  
  def sourceList = _sourceList
  def target = _target
  def targetType = _targetType
  def setProperties = _setProperties
  def hql = _hql
  def partitionColumns = _partitionColumns
  def fileType = _fileType
  def shuffled = _shuffled
  def partitionSize = _partitionSize
  def cached = _cached
  def truncateTarget = _truncateTarget
  
  def sourceList_(sl: List[SourceInfo]) = _sourceList = sl
  def target_(t: String) = _target = t
  def targetType_(tt: String) = _targetType = tt
  def setProperties_(sp: List[String]) = _setProperties = sp
  def hql_(h: String) = _hql = h
  def partitionColumns_(pc: List[String]) = _partitionColumns = pc
  def fileType_(ft: String) = _fileType = ft
  def shuffled_(s: String) = _shuffled = s
  def partitionSize_(ps: Int) = _partitionSize = ps
  def cached_(c: Boolean) = _cached = c
  def truncateTarget_(tt: Boolean) = _truncateTarget = tt
}