# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
#ETL_joinCondn_missing
#ETL_sourceColumn_null
#ETL_target_table_missing
#ETLDocument

import pandas as pd
import xlrd
from xlrd import XLRDError
import re


class spark:
    my_dict = {}
    targetTable = ""
    workbook = xlrd.open_workbook("ETLDocument.xlsx")   # Opening the ETL Document
    worksheet = workbook.sheet_by_index(0)
    df = pd.read_excel(r'ETLDocument.xlsx', skiprows=3, nrows=worksheet.nrows)    # Dataframe of the ETL Document
    transDict = {'populate': ["load", "populate"],         # Dictionary of the Transformation Keywords
                 'uppercase': ["uppercase", "capital"],
                 'lowercase': ["lowercase", "small"]
                 }

    # Parsing the ETL Document
    def parseEtl(self):
        for i in range(3):
            key = self.worksheet.cell_value(i, 0)
            value = []
            value.extend(self.worksheet.cell_value(i, 1).split(","))
            self.my_dict[key] = value

    # Verification of ETL Document
    def sanityCheck(self):

        # check if file in excel format
        try:
            xlrd.open_workbook("ETLDocument.xlsx")
        except XLRDError:
            print("The uploaded file is not in required format.")
            return False
        else:
            print("File format is correct.")

        # check if any null values

        if self.my_dict["Join Condition"] == ['']:
            print("The join condition is missing")
            return False
        if self.my_dict["Source table"] == ['']:
            print("The  Source table is missing")
            return False
        if self.my_dict["Target table"] == ['']:
            print("The target table is missing")
            return False
        df1 = self.df.loc[:, self.df.columns != 'Transformation']
        if df1.isnull().values.any():
            print("Table has missing values")
            return False

        return True

    # importing required scala packages
    def importScalaPackages(self):
        importscript = "import scala.collection.mutable.ListBuffer\nimport org.apache.spark.sql.types.{StructType, " \
                       "StructField, StringType, IntegerType, LongType} "
        f = open("ScalaCode.scala", "w+")
        f.write(importscript)                       # Write the scala script to the file
        f.close()

    # join the source and target
    def joinTables(self):
        s = self.my_dict["Join Condition"][0]
        targetTable = self.my_dict["Target table"][0]
        pattern = r'FROM\s([a-zA-Z]+)\s([a-zA-Z]+)\sJOIN\s([a-zA-Z]+)\sON\s\1.([a-zA-Z]+)\s=\s\3.([a-zA-Z]+)'
        m = re.match(pattern, s)
        table1 = m.group(1)
        table2 = m.group(3)
        condition = m.group(2)
        joinfield1 = m.group(4)
        joinfield2 = m.group(5)
        self.script = "\nval emp = spark.read.format(\"csv\").option(\"header\",\"true\").option(\"inferSchema\"," \
                 "\"true\").load(\"/FileStore/tables/" + table1 + ".csv\")" + " \nval dept = spark.read.format(" \
                 "\"csv\").option(\"header\"," \
                 "\"true\").option(\"inferSchema\"," \
                 "\"true\").load(\"/FileStore/tables/" + \
                 table2 + ".csv\")" + "\nvar " + targetTable + "  = emp.join(dept, Seq(\"" + joinfield1 + "\"), " \
                 "\"" + \
                 condition + "\") "
        f = open("ScalaCode.scala", "a+")
        f.write(self.script)                                # Write the scala script to the file
        f.close()
        print(self.script)

    # Split firstname and lastname name of source table Employee
    def populate(self, sourcecolumn, targetcolumn):
        targetTable = self.my_dict["Target table"][0]
        if targetcolumn.lower() == 'emp firstname':
            a = 0
        elif targetcolumn.lower() == 'emp lastname':
            a = 1
        self.transformationScript1 = "\n\n" + targetTable + " = " + targetTable + "." \
                                     "withColumn(\"" + targetcolumn + "\", split($\"" + sourcecolumn + "\",\" \")." \
                                     "getItem(" + str(a) + ")).drop(\"" + sourcecolumn + "\")"
        f = open("ScalaCode.scala", "a+")
        f.write(self.transformationScript1)                       # Write the scala script to the file
        f.close()
        print(self.transformationScript1)

    # Transform the fields of source column to Uppercase
    def toUpperCase(self, sourcecolumn):
        targetTable = self.my_dict["Target table"][0]
        self.transformationScript2 = "\n\n" + targetTable + " = " + targetTable + "." \
                                "withColumn(\"" + sourcecolumn + "\",upper($\"" + sourcecolumn + "\"))"
        f = open("ScalaCode.scala", "a+")
        f.write(self.transformationScript2)                   # Write the scala script to the file
        f.close()
        print(self.transformationScript2)

    # Transform the fields of source column to Lowercase
    def toLowerCase(self, sourcecolumn):
        targetTable = self.my_dict["Target table"][0]
        self.transformationScript2 = "\n\n" + targetTable + " = " + targetTable + "." \
                                "withColumn(\"" + sourcecolumn + "\",lower($\"" + sourcecolumn + "\"))"
        f = open("ScalaCode.scala", "a+")
        f.write(self.transformationScript2)                 # Write the scala script to the file
        f.close()
        print(self.transformationScript2)

    # Search the fields in the dictionary
    def searchInDict(self, val):
        for key, value in self.transDict.items():
            for i in value:
                if re.search(i, val.lower()):
                    return key

    # Extracting the required fields from the ETL Document
    def transform(self):
        transformation = pd.notnull(self.df["Transformation"])                # Extract the transformation Column
        for ind in self.df[transformation].index:
            st = self.df[transformation]["Transformation"][ind]
            sourcecolumn = self.df[transformation]["Source Column"][ind]      # Extract the Source Column
            targetcolumn = self.df[transformation]["Target Column"][ind]      # Extract the Target Column
            x = self.searchInDict(st)                                         # Search the transformation keywords in dictionary
            if x == 'populate':
                self.populate(sourcecolumn, targetcolumn)
            elif x == 'uppercase':
                self.toUpperCase(sourcecolumn)
            elif x == 'lowercase':
                self.toLowerCase(sourcecolumn)


p = spark()
p.parseEtl()
isValid = p.sanityCheck()
if isValid:                      # if the ETL Document is Valid
    p.importScalaPackages()
    p.joinTables()
    p.transform()
