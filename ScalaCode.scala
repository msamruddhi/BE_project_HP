import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType} 
val emp = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/employee.csv") 
val dept = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/Department.csv")
val jointbl  = emp.join(dept, Seq("Departmentno"), "INNER")

val tmpnameDF = jointbl.withColumn("Emp Firstname", split($"Empname"," ").getItem(0)).drop("Empname")

val tmpnameDF = jointbl.withColumn("Emp LastName", split($"Empname"," ").getItem(1)).drop("Empname")

var capitaldf=jointbl.withColumn("Address",lower($"Address"))

var capitaldf=jointbl.withColumn("Dname",upper($"Dname"))