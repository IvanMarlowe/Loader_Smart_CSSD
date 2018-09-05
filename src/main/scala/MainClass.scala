
import helper.OutputGenerator
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
object MainClass{
  def main(args: Array[String]) {
    val JSONFile = args(0)//"C:/Users/Solvento/workspace/loader_smart_CSSD/config_file.json"/*args(1)*/
    val isUnitTest = args(1).toBoolean
    //set as global argument  
    val argument = new ArgumentLocal
    argument.jsonFile_(JSONFile)
    argument.isUnitTest_(isUnitTest)
    
    ArgumentGlobal.jsonFile_(JSONFile)
    ArgumentGlobal.isUnitTest_(isUnitTest)
    val outputGenerator = new OutputGenerator(argument)
    
    outputGenerator.ingestFile//execute ingest
  }
}