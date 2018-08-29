
import helper.OutputGenerator
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
object MainClass{
  def main(args: Array[String]) {
    val JSONFile = args(0)//"C:/Users/Solvento/workspace/loader_smart_CSSD/config_file.json"/*args(1)*/
    val delimiter = args(2)//"|"
    val fileURI = args(1)//"SMS::C:/user/hive/warehouse/tup_seq/list_file_s.txt::C:/Users/Solvento/workspace/loader_smart_CSSD/avro/SmsCDR.avsc::tempo_one"
    //set as global argument  
    val argument = new ArgumentLocal
    argument.fileURI_(fileURI)
    argument.jsonFile_(JSONFile)
    argument.delimiter_(delimiter)
    
    ArgumentGlobal.jsonFile_(JSONFile)
    ArgumentGlobal.fileURI_(fileURI)
    ArgumentGlobal.delimiter_(delimiter)
    
    val outputGenerator = new OutputGenerator(argument)
    
    outputGenerator.ingestFile//execute ingest
  }
}