package model

class Transform {
  private var _processNumber = ""
  private var _targetType = ""
  private var _targetName = ""
  private var _truncateTarget = ""
  private var _isCached:Boolean = _
  private var _targetLocation = ""
  private var _hql = ""
  private var _outputFormat = ""
  def hql = _hql
  def logLocation = ""
  def targetLocation = _targetLocation
  def processNumber = _processNumber
  def isCached = _isCached
  def targetType = _targetType
  def targetName = _targetName
  def truncateTarget = _truncateTarget

  def outputFormat = _outputFormat
  def isCached_(ic: Boolean) = _isCached = ic
  def processNumber_(pn: String) = _processNumber = pn
  def targetType_(tt: String) = _targetType = tt
  def truncateTarget_(tgt: String) = _truncateTarget = tgt
  def targetName_(tn:String) = _targetName = tn
  def targetLocation_(tl: String) = _targetLocation = tl
  def hql_(h: String) = _hql = h
  def outputFormat_(of: String) = _outputFormat = of
}