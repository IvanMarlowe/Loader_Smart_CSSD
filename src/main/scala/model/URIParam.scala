package model

class URIParam(fileURI: String) {
  
  private val splitSource = fileURI.split("::")
  def cdrType = splitSource.apply(0)
  def listFileLocation = splitSource.apply(1)
  def schemaLocation = splitSource.apply(2)
  def tempTableName = splitSource.apply(3)
}