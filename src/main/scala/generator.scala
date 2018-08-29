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
    val colName = args(2)
//    val sparkContext = ContextHelper.getSparkContext()
//    val hiveContext = ContextHelper.getHiveContext
//    val sc = ContextHelper.getSparkContext()
//    val hiveContext = ContextHelper.getHiveContext//new org.apache.spark.sql.hive.HiveContext(new SparkContext(new SparkConf()))
//    val df = hiveContext.range(0, 10/*20000000*/)
    
//    val df2 = df.selectExpr("id new_id","rand(10000) rand_one", "cast(randFunc(1, 2) as string) rand_two")
//    .selectExpr("new_id", "rand_one", "rand_two", "case when rand_two = '1' then 'Y' else 'N' end as rand_three")//, "cast(randFunc(1, 10) as string) rand_four", "rand_one rand_five")//, "cast(randFunc(1, 10) as string) rand_six", "cast(randFunc(1, 10) as string) rand_seven", "cast(randFunc(1, 10) as string) rand_eight", "cast(randFunc(1, 10) as string) rand_nine", "cast(randFunc(1, 10) as string) rand_ten", "(cast(randFunc(1000, 2000) as integer) * rand(100) * 3) rand_eleven")*/
//    .withColumn("file", lit("passing_file"))
//    df2.repartition(1).write.mode("overwrite").parquet("C:/user/hive/warehouse/data_3_columns")//.format("com.databricks.spark.csv").saveAsTable("GEN_CSV_4")
//    val df= hiveContext.read.format("com.databricks.spark.avro").json("/tmp/spark/avro/Sms.avsc")
//    println(df.show(false) + "avro dataset")
//    val rdd = ContextHelper.getSparkContext().parallelize(for {
//        x <- 1 to 3
//        y <- 1 to 2
//    } yield (x, None), 8)
//hiveContext.read.format("").load(List():_*)
//    hiveContext.refreshTable("table_three")
//    hiveContext.sql("msck repair table table_three")
//    println("DATA" + hiveContext.sql("select * from table_five").show(false))
//    hiveContext.sql(
//        "CREATE EXTERNAL TABLE `table_five_one`(                          " +
//        "  `new_id` bigint COMMENT 'Inferred from Parquet file.',    " +
//        "  `rand_one` double COMMENT 'Inferred from Parquet file.',  " +
//        "  `rand_two` string COMMENT 'Inferred from Parquet file.',  " +
//        "  `rand_three` string COMMENT 'Inferred from Parquet file.'," +
//        "  `rand_four` string COMMENT 'Inferred from Parquet file.', " +
//        "  `rand_five` double COMMENT 'Inferred from Parquet file.') " +
//        "stored as parquet                                           " +
//        "location 'C:/user/data/table_sample_one/`part_5*`'"
//        )
//    
//        hiveContext.sql(
//        "CREATE EXTERNAL TABLE `table_three_one`(                          " +
//        "  `new_id` bigint COMMENT 'Inferred from Parquet file.',    " +
//        "  `rand_one` double COMMENT 'Inferred from Parquet file.',  " +
//        "  `rand_two` string COMMENT 'Inferred from Parquet file.',  " +
//        "  `rand_three` string COMMENT 'Inferred from Parquet file.')" +
//        "stored as parquet                                           " +
//        "location 'C:/user/data/table_sample_one/part_3*'"
//        )
//    val sparkContext = ContextHelper.getSparkContext()
////    val df = hiveContext
////    .read
////    .parquet("C:/user/hive/warehouse/gen_parquet/part-r-00000-0d6ca9f3-e43e-4a30-a1c9-fae585ba9245.gz.parquet")
////    .rdd
//    val listStr = List("e00da03b685a0dd18fb6a08af0923de0,0.22769709082546485,2,N,8,0.22769709082546485,8,8,8,8,8,2624.589568161663,passing_file" ,"c86a7ee3d8ef0b551ed58e354a836f2b,0.850991102552251,2,N,4,0.850991102552251,4,4,4,4,4,4692.529742775384,passing_file" ,"0f96613235062963ccde717b18f97592,0.3211493900659459,1,Y,8,0.3211493900659459,8,8,8,8,8,3707.6081695871217,passing_file" ,"07871915a8107172b3b5dc15a6574ad3,0.46726216777119156,2,N,6,0.46726216777119156,6,6,6,6,6,1517.2935897702796,passing_file" ,"184260348236f9554fe9375772ff966e,0.6489816349307717,1,Y,3,0.6489816349307717,3,3,3,3,3,1359.7618859748375,passing_file" ,"2bd7f907b7f5b6bbd91822c0c7b835f6,0.2572000823095545,1,Y,2,0.2572000823095545,2,2,2,2,2,1213.3560215301493,passing_file" ,"3c1e4bd67169b8153e0047536c9f541e,0.06452282974130197,1,Y,7,0.06452282974130197,7,7,7,7,7,1071.4702593745296,passing_file" ,"68148596109e38cf9367d27875e185be,0.5614068621927615,2,N,4,0.5614068621927615,4,4,4,4,4,390.38047167593527,passing_file" ,"37d097caf1299d9aa79c2c2b843d2d78,0.13379921599474232,1,Y,6,0.13379921599474232,6,6,6,6,6,3201.6126458944536,passing_file" ,"063e26c670d07bb7c4d30e6fc69fe056,0.4436946037043823,1,Y,1,0.4436946037043823,1,1,1,1,1,4654.225441476496,passing_file" ,"09a5e2a11bea20817477e0b1dfe2cc21,0.8186473954097021,2,N,6,0.8186473954097021,6,6,6,6,6,3842.054824405781,passing_file" ,"7f018eb7b301a66658931cb8a93fd6e8,0.6619404153698099,1,Y,4,0.6619404153698099,4,4,4,4,4,2264.603138806316,passing_file" ,"e8bf0f27d70d480d3ab793bb7619aaa5,0.7201056262274418,2,N,7,0.7201056262274418,7,7,7,7,7,2753.076306191345,passing_file" ,"8643c8e2107ba86c47371e037059c4b7,0.82060140859131,2,N,2,0.82060140859131,2,2,2,2,2,1202.1680720923955,passing_file" ,"ba304f3809ed31d0ad97b5a2b5df2a39,0.33789905836507617,1,Y,1,0.33789905836507617,1,1,1,1,1,1504.2782770956944,passing_file")
////    val rdd = sparkContext.sequenceFile(path, keyClass, valueClass)
//    val tupList = listStr.zipWithIndex.map{
//      case(x, y) => {
//        (y, x)
//      }
//    }
    
//    sparkContext.parallelize(tupList).repartition(1).saveAsSequenceFile("C:/user/hive/warehouse/tup_seq")
//    rdd.saveAsSequenceFile("C:/user/hive/warehouse/gen_seq")
//    hiveContext.sql("msck repair table tempo_one_table")
//    hiveContext.sql("select * from tempo_one_table").show(false)
//    hiveContext.range(2).withColumn("trans_1", lit(rand() * 1000000)).withColumn("country", lit("Singapore")).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))
//    .unionAll(
//      hiveContext.range(100000).withColumn("country", lit("China")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .unionAll(
//      hiveContext.range(10000).withColumn("country", lit("America")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .unionAll(
//      hiveContext.range(20000).withColumn("country", lit("Japan")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .unionAll(
//      hiveContext.range(500).withColumn("country", lit("Indonesia")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .unionAll(
//      hiveContext.range(12500).withColumn("country", lit("Singapore")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .unionAll(
//      hiveContext.range(100).withColumn("country", lit("Taiwan")).withColumn("trans_1", lit(rand() * 1000000)).withColumn("trans_2", lit(rand() * 10000)).withColumn("trans_3", lit(rand() * 52500))    
//    )
//    .write
//    .mode("append")
//    .parquet("C:/user/hive/warehouse/gen_latest_july16_1")
//    .saveAsTable("tempo_one_table")
//    .parquet("C:/user/hive/warehouse/gen_latest_july13")
//   val seq = sparkContext.sequenceFile("C:/user/hive/warehouse/gen_seq", classOf[IntWritable], classOf[Text])
//   val dataCSV = seq.map(data => (data._2.toString())).saveAsTextFile("C:/user/hive/warehouse/csv_file_complete")
   
//   val a = seq.collect()
//    hiveContext.range(1).write.saveAsTable("num_sample")
    
    
//    val data = sc.sequenceFile("/data/oneocs/normal/sms/20180801/ONEOCS-IDC-normal-SMS-2018080112-RAW01_20180807194146101.sf", classOf[IntWritable], classOf[BytesWritable])//.map(x => Text.decode(x._2.copyBytes()).split("\n"))
//    data.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, codec)
//     val data = sc
//     .sequenceFile("C:/Users/Solvento/Downloads/ONEOCS-IDC-normal-VOU-2018080112-RAW01_20180807194140101.sf", classOf[IntWritable], classOf[BytesWritable])
//     .map(x => Text.decode(x._2.copyBytes()).split("\n"))
//     .collect
//     .flatten
//     
//     println(data.size + " WEW")
////     .map(wew)
//     
     
//     sc.parallelize(data).saveAsTextFile("C:/user/hive/warehouse/VOUCHER.CSV")
     
     
     
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val x = for (key <- 1 to 1000000) yield (key, 1)
    
//    sc.parallelize(x).toDF("key", "value").registerTempTable("df")
//    import scala.util.Random
//    sc.parallelize(Random.shuffle(x)).toDF("key", "value").registerTempTable("df1")
//    sc.parallelize(Random.shuffle(x)).toDF("key", "value").registerTempTable("df2")
    
//    hiveContext.sql("CACHE TABLE df")
    
//    hiveContext.sql("select rand(10)").show(false)
//    val rangeDf = hiveContext.range(1000000).selectExpr("id", "floor(rand(10) * 10) random").registerTempTable("df")
//    hiveContext.sql("select distinct random from df").show(false)
//    val newDf = hiveContext.sql("select * from df").repartition(col("random"))
//    println(newDf.rdd.partitions)
//    val newDf2 = hiveContext.sql("select * from df")
//    println(newDf2.rdd.getNumPartitions + " WEW 2")
//    sqlContext.sql("SELECT * FROM df cluster by key JOIN df1 ON df.key = df1.key ").show
//    hiveContext.sql("SELECT * FROM df cluster by key").show(false)
//    sqlContext.sql("SELECT * FROM df JOIN df2 ON df.key = df2.key").show
//    System.in.read()
     
//     hiveContext.range(1000000).selectExpr("cast((id + 2000000) as String) TransID").selectExpr("TransID", "rand(1000) Random", "'1' ISOCODE", "'3' CodeNum", "'PHP' Currency", "'Philippines' Country")
//     .unionAll(
//        hiveContext.range(1000000).selectExpr("cast((id + 3000000) as String) TransID").selectExpr("TransID", "rand(1000) Random", "'2' ISOCODE", "'4' CodeNum", "'USD' Currency", "'United States' Country") 
//     )
//     .unionAll(
//       hiveContext.range(1000000).selectExpr("cast((id + 4000000) as String) TransID").selectExpr("TransID", "rand(1000) Random", "'3' ISOCODE", "'5' CodeNum", "'YEN' Currency", "'Japan' Country")    
//     )
//     .unionAll(
//       hiveContext.range(1000000).selectExpr("cast((id + 5000000) as String) TransID").selectExpr("TransID", "rand(1000) Random", "'5' ISOCODE", "'5' CodeNum", "'NZD' Currency", "'New Zealand' Country")
//     )
//     .write.mode("append")
//     .partitionBy("country
//     .saveAsTable("country_data")
     
//     hiveContext.sql("select * from country_data where country = 'Singapore'").show(false)
     
//     hiveContext.range(1000000).selectExpr("cast((id + 2000000) as String) TransID").selectExpr("TransID", "rand(1000) Random", "'1' ISOCODE", "'3' CodeNum", "'PHP' Currency", "'Philippines' Country")
//     .registerTempTable("a")
//     sc.
//     hiveContext.sql("select * from (select * from a join (select 'wewers') b where Currency  = 'PHP') c cluster by Random").show(false)
    
//    val newRDD1 = sc.sequenceFile("/data/oneocs/normal/sms/20180801/ONEOCS-IDC-normal-SMS-2018080112-RAW01_20180807194146101.sf", classOf[IntWritable], classOf[BytesWritable], 20).flatMap(data => {
//  new String(data._2.copyBytes(), StandardCharsets.UTF_8)
//  .split("\n")
//  .map(str => str.split("\\|").drop(2).mkString("|"))
//})
//     val partFiles = fs.globStatus(new Path("C:/user/hive/warehouse" + "/*"))
//     
//     partFiles.take(1).map(data => {
//       println(data.getPath.toString)
//     })
    
//    println("hdfs dfs -ls /".!)
    
//    fs.globStatus(new Path("C:/user/hive/warehouse" + "/*")).
//     val dataSize = FileSystem.getF
//     .getUsed
     
//     println(dataSize + " WEW")
      ContextHelper.getHiveContext.read.parquet(parquetSourceLoc).repartition(10)
      .withColumn("ld_dt", expr("substr("+ colName + ", 0, 8)"))
      .withColumn("ld_hr", expr("substr(" + colName + ", 9, 2)")).withColumnRenamed(colName, "strt_dt")
      .write
      .mode("append")
      .partitionBy("ld_dt", "ld_hr")
      .parquet(parquetTgtLoc)

//     ContextHelper.getHiveContext.sql("").sort
  }
  
}