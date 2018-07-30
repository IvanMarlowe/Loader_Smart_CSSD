package UDF
import java.util.Date
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.text.SimpleDateFormat;
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.security.MessageDigest
import org.apache.spark.sql.Column
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe
import org.apache.spark.sql.Row
import java.math.BigInteger
import Util.DataManipulationFunctions
class Functions(oozieId: String) {
    
  //hive md5 converter
  val md5Converter: (String => String) = (s) => { 
    var x = s
     if(x == null){
       null
     } 
     else{
       val digest = MessageDigest.getInstance("MD5")  
       val md5hash = digest.digest(x.getBytes).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}  
       md5hash.toLowerCase()
     }
  }

  def md5Convert = udf(md5Converter)
   
   val newHashKeyConverter:((String, String) => String) = (pk, oozieId) => {
     val convertedNow = new DataManipulationFunctions().generatedFormattedNow()
     pk.concat(convertedNow.concat(oozieId))
  }
   
   val nowGenerator: ( () => String) = () =>   {
     lazy val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
     val convertedNow = sdf.format(new Date)
     convertedNow
   }
   
   val genRand:((String, String) => String) = (start, end) => {
     val r = new scala.util.Random
     val r1 = start.toInt + r.nextInt(( end.toInt - start.toInt) + 1)
     r1.toString()
   }
   
   def randFunc = udf(genRand)
   
   val newHashKeyConvert = udf(newHashKeyConverter)
   val decodeDoubleConvert = udf(decodeDouble)
   val decodeDouble:(Double => String) = (data) => {
     if(data <= 0.1)
       "one"
     else if(data <= 0.2)
       "two"
     else if(data <= 0.3)
       "three"
     else if(data <= 0.4)
       "four"
     else if(data <= 0.5)
       "five"
     else if(data <= 0.6)
       "six"
     else if(data <= 0.7)
       "seven"
     else if(data <= 0.8)
       "eight"
     else if(data <= 0.9)
       "nine"
     else
       "wow"
   }
   
   val combine:(Seq[String] => String) = (data) => {
     data.mkString
   }
   
   def combineFunc = udf(combine)
   
   val computeDouble:(Double => Double) = (data) => {
     data * data * data * 200
   }
}