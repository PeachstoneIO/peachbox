import unittest
import peachbox.dwh
import peachbox.fs

class TestDWH(unittest.TestCase):
    def test_init(self):
        with peachbox.DWH(fs=peachbox.fs.Local()) as dwh:
            self.assertIsInstance(dwh, peachbox.dwh.DWH)

    def test_access_by_instance(self):
        with peachbox.DWH() as dwh1:
            dwh2 = peachbox.DWH.Instance()
            self.assertEqual(dwh1,dwh2)

    #def test_instanciation_by_instance(self):
        #with peachbox.spark.Instance() as spark:
            #self.assertIsInstance(spark, peachbox.spark.DWH)







        

