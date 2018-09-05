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
object generator{
  def main(args: Array[String]) {
    
    
    val parquetSourceLoc = args(0)
    val parquetTgtLoc = args(1)
    val hql = args(2)
    ContextHelper.getHiveContext.read.parquet(parquetSourceLoc).registerTempTable("temporary_table")
    ContextHelper.getHiveContext.sql(hql).write.mode("append").parquet(parquetTgtLoc)
    println("DATA SAVED")
  }
  
}