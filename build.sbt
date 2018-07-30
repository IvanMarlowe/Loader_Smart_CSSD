version := "1.0"

scalaVersion := "2.10.5"



libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-sql_2.10" % "1.6.3",
    "org.apache.spark" % "spark-core_2.10" % "1.6.3",
    "org.apache.spark" % "spark-hive_2.10" % "1.6.3", 
    "org.apache.spark" % "spark-streaming_2.10" % "1.6.3",
	"com.databricks" % "spark-csv_2.10" % "1.5.0"
)
