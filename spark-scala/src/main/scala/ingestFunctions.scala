import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import sys.process._;

object ingestFunctions {

	def readFunctions(spark:SparkSession,format:String="csv",location:String ="s3://zz-testing/jcher2/csv/test.csv",table:String="test",where_clause:String="1=1"): DataFrame = {
		var df =spark.sql("select 'Non Configured Format' ")
		var query="";
		println (format);
		if (format == "csv"){
			println (location)
			df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(location)
		}
		if (format == "parquet"){
			println (location)
			df = spark.read.parquet(location)
		}
		if (format == "avro"){
			println (location)
			df = spark.read.format("com.databricks.spark.avro").load(location)
		}
		if (format == "avro"){
			println (location)
			df = spark.read.format("com.databricks.spark.avro").load(location)
		}
		if (format == "hive"){
			println (location)
			if (where_clause !="1=1"){
				df=spark.sql("select * from table ")

			}
			else {
				query="select * from df where "+where_clause
				df=spark.sql(query)
			}
		}
		df.registerTempTable("df")
		if (format != "hive" && where_clause !="1=1"){
			query="select * from df where "+where_clause
			df=spark.sql(query)
		}
		return df
	}

}








//val df1=readFunctions(format="hive",table="dsmsca_processed.inventory_allocation_summary_detail")
//val df1=readFunctions()
