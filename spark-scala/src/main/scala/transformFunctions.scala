import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.sys.process._
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;

object transformFunctions {
	def commonTransform(spark:SparkSession,df: DataFrame,transformation:String="collect()"): DataFrame = {
		df.registerTempTable("df")
		var transformation=s"""select * from df"""
		var df_final=spark.sql(transformation)
		return df_final

	}
}


/*

val arraySchema = new StructType().add("name",StringType).add("knownLanguages", ArrayType(StringType)).add("properties", MapType(StringType,StringType))
df.select($"name",$"properties",explode($"knownLanguages")).show(false)



import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
    Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
    Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
    Row("Washington",null,null),
    Row("Jefferson",List(),Map())
    )

val arraySchema = new StructType().add("name",StringType).add("knownLanguages", ArrayType(StringType)).add("properties", MapType(StringType,StringType))
val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)


*/
