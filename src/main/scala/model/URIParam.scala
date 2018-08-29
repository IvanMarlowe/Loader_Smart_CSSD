package model

class URIParam(fileURI: String) {
  
  private val splitSource = {
    val split = fileURI.split("::")
    if(split.size < 5){
      throw new Exception("Not enough parameters for the URI of the List File Information.")
    }
    else{
      split
    }
  }
  
  def cdrType = splitSource.apply(0)
  def listFileLocation = splitSource.apply(1)
  def schemaLocation = splitSource.apply(2)
  def tempTableName = splitSource.apply(3)
  def isCached = splitSource.apply(4).toLowerCase().toBoolean
  
  

}