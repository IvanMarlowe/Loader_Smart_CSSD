
import helper.OutputGenerator
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
object MainClass{
  def main(args: Array[String]) {
    val JSONFile = "C:/Users/Solvento/workspace/loader_smart_CSSD/config_file.json"/*args(1)*/
//    val cdrType =  "SAMPLE_1"
    val delimiter = ","
    val fileURI = "SAMPLE_1::C:/user/hive/warehouse/tup_seq/list_file.txt::C:/Users/Solvento/workspace/loader_smart_CSSD/avro/Sms.avsc::tempo_one"
    //set as global argument  
    val argument = new ArgumentLocal
    argument.fileURI_(fileURI)
    argument.jsonFile_(JSONFile)
//    argument.cdrType_(cdrType)
    argument.delimiter_(delimiter)
    
    ArgumentGlobal.jsonFile_(JSONFile)
//    ArgumentGlobal.cdrType_(cdrType)
    ArgumentGlobal.fileURI_(fileURI)
    ArgumentGlobal.delimiter_(delimiter)
    
    val outputGenerator = new OutputGenerator(argument)
    
    outputGenerator.ingestFile//execute ingest
  }
}