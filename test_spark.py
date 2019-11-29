from unittest import TestCase
from SparkCodeGenNew import spark


class TestSpark(TestCase):
    ob = spark()

    # Test the sanityCheck function
    def test_sanityCheck(self):
        print("SANITY CHECK ")
        res = self.ob.sanityCheck()
        print(res)
        self.assertTrue(res)

    # Test the populate function
    def test_populate(self):
        print("POPULATE ")
        sc = "Empname"
        tc = "Emp FirstName"
        self.ob.populate(sc, tc)
        expected = "\n\nEmpDeptDetails = EmpDeptDetails.withColumn(\"Emp FirstName\", split($\"Empname\",\" \").getItem(0)).drop(\"Empname\")"
        print(self.ob.transformationScript1)
        self.assertEqual(self.ob.transformationScript1, expected)

    # Test the toUpperCase function
    def test_toUpperCase(self):
        print("TO UPPER CASE ")
        sc = "Dname"
        self.ob.toUpperCase(sc)
        expected = "\n\nEmpDeptDetails = EmpDeptDetails.withColumn(\"Dname\",upper($\"Dname\"))"
        self.assertEqual(self.ob.transformationScript2, expected)

    # Test the toLowerCase function
    def test_toLowerCase(self):
        print("TO LOWER CASE ")
        sc = "Address"
        self.ob.toLowerCase(sc)
        expected = "\n\nEmpDeptDetails = EmpDeptDetails.withColumn(\"Address\",lower($\"Address\"))"
        self.assertEqual(self.ob.transformationScript2, expected)
