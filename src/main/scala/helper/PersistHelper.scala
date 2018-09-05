package helper
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import model.{SourceInfo, ConfigFileV3, Transform}
import sys.process._

object PersistHelper {
  def transferFile(outputTmpDir: String, targetLocation: String, transform: Transform){
     val fs = FileSystem.get(ContextHelper.getSparkContext().hadoopConfiguration)
     val partFiles = fs.globStatus(new Path(outputTmpDir + "/*"))
     val partFilesTarget = fs.globStatus(new Path(targetLocation + "/*"))
     val isTruncate = transform.truncateTarget
     
     if(isTruncate.equals(true)){
       partFilesTarget.map(data => fs.delete((new Path(data.getPath.toString)), true))
     }
     
     ("hdfs dfs -mkdir " + targetLocation).!
     
     partFiles.map(data => {
             
      val strData = data.getPath.toString().split("/").last
      val absPath = outputTmpDir
      val newStrData = strData + "_" + System.currentTimeMillis().toString
      
      fs.rename(new Path(absPath + "/" + strData), new Path(targetLocation + "/" + newStrData))
     })
     
     CleanupHelper.addToDelete(outputTmpDir)
     
     println("Files moved successfully to '" + targetLocation + "'")
  }
  
  def saveCSVFile(configFile: ConfigFileV3, transform: Transform, data: DataFrame){
    val dataName = transform.target.split("/").last
    val cached = transform.cached
    val shuffled = transform.shuffled
    val partitionCol = transform.partitionColumns
    val target = transform.target
    val tmpLocation = configFile.envLocation.concat("/" + ContextHelper.getSparkContext().applicationId).concat("_").concat(dataName)
    
    data
    .write
    .format("com.databricks.spark.csv")
    .save(tmpLocation)
    
    transferFile(tmpLocation, target, transform)
  }
}