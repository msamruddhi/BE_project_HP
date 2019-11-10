# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 14:17:09 2019

@author: Samruddhi Malge
"""

import pandas as pd
import xlrd
from xlrd import XLRDError
import re
my_dict={}
workbook= xlrd.open_workbook("ETLDocument.xlsx")
worksheet = workbook.sheet_by_index(0)
df=pd.read_excel(r'ETLDocument.xlsx',skiprows=3,nrows=worksheet.nrows)

def parse_etl():
  for i in range(3):
      key=worksheet.cell_value(i,0)
      value=[]
      value.append(worksheet.cell_value(i,1).split(","))
      my_dict[key]=value
     
def sanity_check():
 
  #check if file in excel format
  try:
    xlrd.open_workbook("ETLDocument.xlsx")
  except XLRDError:
    print("The uploaded file is not in required format.")
  else:
    print("File format is correct.")
   
  #check if any null values
  flag=0
  for i in range(3):
    if(worksheet.cell_type(i,0)==xlrd.XL_CELL_EMPTY or worksheet.cell_type(i,1)==xlrd.XL_CELL_EMPTY):
      print("The uploaded file has null value in first 3 lines")
      flag=1
      break
  df1 = df.loc[:, df.columns != 'Transformation']
  if(flag==0):
    if(df1.isnull().values.any()):
      flag=1
      print("The uploaded file has null value")

   
 
def ImportScalaPackages():
  importscript = "import scala.collection.mutable.ListBuffer\n\nimport org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType} "
  f=open("ScalaCode.scala","w+")
  f.write(importscript)
  f.close()
     
def join_tables():
  s = my_dict["Join Condition"][0][0]
  pattern = r'FROM\s([a-zA-Z]+)\s([a-zA-Z]+)\sJOIN\s([a-zA-Z]+)\sON\s\1.([a-zA-Z]+)\s=\s\3.([a-zA-Z]+)'
  m = re.match(pattern, s)
  table1=m.group(1)
  table2=m.group(3)
  condition = m.group(2)
  joinfield1 = m.group(4)
  joinfield2 = m.group(5)
  script = "\nval emp = spark.read.format(\"csv\").option(\"header\",\"true\").option(\"inferSchema\",\"true\").load(\"/FileStore/tables/"+table1+".csv\")"+" \nval dept = spark.read.format(\"csv\").option(\"header\",\"true\").option(\"inferSchema\",\"true\").load(\"/FileStore/tables/"+table2+".csv\")"+"\nval jointbl  = emp.join(dept, Seq(\""+joinfield1+"\"), \""+condition+"\")"
  f=open("ScalaCode.scala","a+")
  f.write(script)
  f.close()
  print(script)

def populate(sourcecolumn,targetcolumn):
  if targetcolumn.lower()=='emp firstname':
    a=0
  elif targetcolumn.lower() == 'emp lastname':
    a=1
  trans_script1 = "\n\nval tmpnameDF = jointbl.withColumn(\""+targetcolumn+"\", split($\""+sourcecolumn+"\",\" \").getItem("+str(a)+")).drop(\""+sourcecolumn+"\")"
  f=open("ScalaCode.scala","a+")
  f.write(trans_script1)
  f.close()
  print(trans_script1)
 
   
     
def capital(sourcecolumn):
  trans_script2="\n\nvar capitaldf=jointbl.withColumn(\""+sourcecolumn+"\",upper($\""+sourcecolumn+"\"))"
  f=open("ScalaCode.scala","a+")
  f.write(trans_script2)
  f.close()
  print(trans_script2)
 
def smallcase(sourcecolumn):
  trans_script2="\n\nvar capitaldf=jointbl.withColumn(\""+sourcecolumn+"\",lower($\""+sourcecolumn+"\"))"
  f=open("ScalaCode.scala","a+")
  f.write(trans_script2)
  f.close()
  print(trans_script2)
 

def transform():
  transformation=pd.notnull(df["Transformation"])
  for ind in df[transformation].index:
      st=df[transformation]["Transformation"][ind]
      sourcecolumn=df[transformation]["Source Column"][ind]
      targetcolumn=df[transformation]["Target Column"][ind]
      if re.search("populate",st.lower()) or re.search("load",st.lower()):
          populate(sourcecolumn,targetcolumn)
      elif re.search("capital",st.lower()) or re.search("uppercase",st.lower()):
          capital(sourcecolumn)
      elif re.search("small",st.lower())or re.search("lowercase",st.lower()):
          smallcase(sourcecolumn)
         
     

parse_etl()
sanity_check()
ImportScalaPackages()
join_tables()

transform()