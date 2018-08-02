package model

class SourceInfo {
  private var _dataLocation: String = _
  private var _type: String = _
  private var _schemaLocation: String = _
  private var _name: String = _
  
  def name = _name
  def dataLocation = _dataLocation
  def schemaLocation = _schemaLocation
  def typing = _type
  
  
  def name_(n: String) = _name = n
  def typing_(t: String) = _type = t
  def dataLocation_(l: String) = _dataLocation = l
  def schemaLocation_(l: String) = _schemaLocation = l
}