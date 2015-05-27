from peachbox.connector.sink import RealTimeView

class BatchView(RealTimeView):
    pass

#    def absorb(self, data_descriptor):
#        # hbase_outputformat <host> test row1 f1 q1 value1
#        # sc.parallelize([sys.argv[3:]]).map(lambda x: (x[0], x))
#        # ['row1', 'f1', 'q1', 'value1'] -> ('row1', ['row1', 'f1', 'q1', 'value1']])
#        #
#
#        host  = 'localhost' 
#        model = data_descriptor['model']
#        data  = data_descriptor['data']
#        table = model.name()
#
#        sc = peachbox.Spark.Instance().context()
#
#        conf = {"hbase.zookeeper.quorum": host,
#                "hbase.mapred.outputtable": table,
#                "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
#                "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
#                "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
#
#        keyConv   = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
#        valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
#
#        #sc.parallelize([('12345',['1234', 'f1','r1','r3'])]).saveAsNewAPIHadoopDataset(
#        data.saveAsNewAPIHadoopDataset(
#                conf=conf,
#                keyConverter=keyConv,
#                valueConverter=valueConv)
#
#    def set_param(self, param):
#        pass
#
