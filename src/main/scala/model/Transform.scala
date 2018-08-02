package model

class Transform {
  private var _processNumber = ""
  private var _targetType = ""
  private var _targetName = ""
  private var _truncateTarget = ""
//  private var _persistToTarget = ""
//  private var _endDateSource = ""
  private var _targetLocation = ""
  private var _hql = ""
  private var _outputFormat = ""
  def hql = _hql
  def logLocation = ""
  def targetLocation = _targetLocation
  def processNumber = _processNumber
//  def database = _database
  def targetType = _targetType
  def targetName = _targetName
  def truncateTarget = _truncateTarget
//  def persistToTarget = _persistToTarget
//  def endDateSource = _endDateSource
  def outputFormat = _outputFormat
  def processNumber_(pn: String) = _processNumber = pn
//  def database_(db: String) = _database = db
  def targetType_(tt: String) = _targetType = tt
  def truncateTarget_(tgt: String) = _truncateTarget = tgt
//  def persistToTarget_(ptt: String) = _persistToTarget = ptt
  def targetName_(tn:String) = _targetName = tn
//  def endDateSource_(eds: String) = _endDateSource = eds
  def targetLocation_(tl: String) = _targetLocation = tl
  def hql_(h: String) = _hql = h
  def outputFormat_(of: String) = _outputFormat = of
}