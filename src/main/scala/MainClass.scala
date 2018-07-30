
import helper.OutputGenerator
import helper.ContextHelper
import model.{ArgumentLocal, ArgumentGlobal}
object MainClass{
  def main(args: Array[String]) {
    val listFileLocation = "C:/user/hive/warehouse/tup_seq/list_file.txt"///*args(0)*/"C:/user/hive/warehouse/smart_July5"//location of output parquet
    val JSONFile = /*args(1)*/"C:/Users/Solvento/workspace/loader_smart_seq_file/config_file.json"/*args(1)*/
    val delim = ","/*args(2)*/
    val cdrType = "SAMPLE_1"/*args(3)*/
    //set as global argument  
    val argument = new ArgumentLocal
    argument.jsonFile_(JSONFile)
    argument.location_(listFileLocation)
    argument.delimiter_(delim)
    argument.cdrType_(cdrType)
    
    ArgumentGlobal.delimiter_(delim)
    ArgumentGlobal.location_(listFileLocation)
    ArgumentGlobal.jsonFile_(JSONFile)
    ArgumentGlobal.cdrType_(cdrType)
    
    val outputGenerator = new OutputGenerator(argument)
    
    outputGenerator.ingestFile//execute ingest
  }
}