package model

class SourceInfo {
 private var _source = ""
 private var _metadata = ""
 private var _fileDelimiter = ""
 private var _fileType = ""
 private var _sourceType = ""
 private var _partitionSize:Int = 0
 private var _shuffled = ""
 private var _cached:Boolean = false
 private var _name = ""
 
 def source = _source
 def name = _name
 def metadata = _metadata
 def fileDelimiter = _fileDelimiter
 def fileType = _fileType
 def sourceType = _sourceType
 def partitionSize = _partitionSize
 def shuffled = _shuffled
 def cached = _cached
 
 def name_(n: String) = _name = n
 def cached_(c: Boolean)  = _cached = c
 def source_(s: String) = _source = s
 def metadata_(m: String) = _metadata = m
 def fileDelimiter_(fd: String) = _fileDelimiter = fd
 def fileType_(ft: String) = _fileType = ft
 def sourceType_(st: String) = _sourceType = st
 def partitionSize_(pt: Int) = _partitionSize = pt
 def shuffled_(s: String) = _shuffled = s
}