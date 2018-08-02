
import helper.OutputGenerator
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
object MainClass{
  def main(args: Array[String]) {
    val JSONFile = "C:/Users/Solvento/workspace/loader_smart_CSSD/config_file.json"/*args(1)*/
    val cdrType =  "SAMPLE_1"
    //set as global argument  
    val argument = new ArgumentLocal
    argument.jsonFile_(JSONFile)
    argument.cdrType_(cdrType)
    
    ArgumentGlobal.jsonFile_(JSONFile)
    ArgumentGlobal.cdrType_(cdrType)
    
    val outputGenerator = new OutputGenerator(argument)
      
    outputGenerator.ingestFile//execute ingest
    System.in.read()
  }
}