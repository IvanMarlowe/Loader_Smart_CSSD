package model

class ColumnMapping(name: String, mapping: String) {
  private val _name:String = name
  private val _mapping:String = mapping
  
  def name() = _name
  def mapping() = _mapping
  
}