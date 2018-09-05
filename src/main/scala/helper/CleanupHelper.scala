package helper
import model.ArgumentGlobal

object CleanupHelper {
  private var _deletionDetails = List[String]()
  
  def addToDelete(location: String) =  {
    if(ArgumentGlobal.isUnitTest().equals(false)){
      _deletionDetails = _deletionDetails:+location
    }
  }
  
  def cleanup() = _deletionDetails.map(data => DataManipulator.deleteIfExistsFile(data))
}