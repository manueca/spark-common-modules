import io.delta.tables._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import scala.sys.process._

object common_functions {
	
	def hdfs_write(spark:SparkSession,df: DataFrame, partition_column: String, lowest_part_col: String,
               hdfs_target: String, s3_target: String, partition_high_col :String){
		var s3_target_final=s3_target;
		var hdfs_target_final=hdfs_target;

	    //spark.logger.info("Starting write to HDFS")
	    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
	    //var hdfs_target="/data/test";
	    //var s3_target="s3://zz-testing/jcher2/po_base_final"
	    //var lowest_part_col="snapshot_dt";
	    if (hdfs_target.takeRight(1) != "/" ){
	    	var hdfs_target_final=hdfs_target+"/";
	    }
	    
	    df.write.format("parquet").mode("overwrite").partitionBy(partition_column).save(hdfs_target_final)
	    //Delete old data from s3 for the partitions processed (where data present in HDFS)
	    var bash_command_get_parts = "hdfs dfs -ls -R " + hdfs_target_final + " | sed '/^-/d' | sed '/.*" + lowest_part_col + ".*/!d' | awk '{print $8}'| sed 's#" + hdfs_target_final + "##'"
		var cmd_1="hdfs dfs -ls -R  "+hdfs_target_final
		var cmd_2 = "sed /^-/d"
		var cmd_3 = "sed /.*snapshot_dt.*/!d "
		var cmd_4 = "sed s#/data/test##"
		var cmd = cmd_1 #| cmd_2 #| cmd_3 #| cmd_4
		val output = cmd.!!
		//val partition_high_col="account_type"
		val replace_val=".*/"+partition_high_col
		var out_formaatted=output.replaceAll(replace_val,partition_high_col)
		var loc=out_formaatted.split("\n")

	    if (s3_target.takeRight(1) != "/" ){
	    	 s3_target_final=s3_target+"/";
	    }
	    //spark.logger.info(s3_target)
	    for (locs <- loc){
	    	var del_command="aws s3 rm " + s3_target_final + locs + "/" + " --recursive --page-size 800"
	    	del_command.!
	    }

	}	

}



