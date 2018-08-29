import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop._
import org.apache.spark.sql.SQLContext
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.DataFrame
import helper.ContextHelper
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Encoder, Encoders}
import model.ColumnMapping
import sys.process._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Encoders.kryo
import helper.DataManipulator
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.rdd.SequenceFileRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{IntWritable, BytesWritable, Text}
import scala.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs._
import org.json4s.jackson.JsonMethods.parse
import scala.io.Source.fromURL

object sample {


  def main (args: Array[String]) {
    val hc = ContextHelper.getHiveContext
//    hc.sql("select 'ivan' name, 'marlowe' asd, 1 id").write.saveAsTable("schema_tempo")
//    hc.sql("drop table schema_tempo")
    hc.sql("create external table schema_tempo4 (id string, name string, asd string) stored as parquet location 'C:/user/hive/warehouse/schema_tempo'")
//    hc.sql("select 20000 id,'pine' name,  'fruitas' asd").write.mode("append").saveAsTable("wew_yoko_na")
//    println(hc.sql("select * from wew_yoko_na").show(false) + "WEW")
    println(hc.sql("select * from schema_tempo4 where name = 'ivan samk'").show(false) + " WEW 2")
//    println(hc.read.parquet("C:/user/hive/warehouse/schema_tempo/part-r-00000-2416999d-3e2b-494a-8c27-fd78d509c063.snappy.parquet").show(false) + " WEW")
  }

}