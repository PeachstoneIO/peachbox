from random import random
from subprocess import call
import json
import glob
import os

class TestHelper(object):
    @staticmethod
    def mkdir_tmp():
        dir_name = '/tmp/' + TestHelper.random_name() 
        call(['mkdir', dir_name])
        return dir_name

    @staticmethod
    def touch(path):
        call(['touch', path])
    
    @staticmethod
    def mkdir(path):
        TestHelper.mkdir_p(path)

    @staticmethod
    def mkdir_p(path):
        call(['mkdir', '-p', path])

    @staticmethod
    def rm_r(dir_name):
        call(['rm', '-fr', dir_name])

    @staticmethod
    def write_json(filename, json_input, dir_name=''):
        if not dir_name: dir_name = TestHelper.create_temp_dir()

        if( not os.path.exists(dir_name)): call(['mkdir', '-p', dir_name]) 

        filename = dir_name + '/' + filename
        if filename.endswith('.gz'): filename = filename[0:-3]
        print "writing json to: " + filename
        f = open(filename, 'w')
        for line in json_input:
            f.write(json.dumps(line) + '\n')
        f.close()
        call(['gzip', filename])
        return filename + '.gz'

    @staticmethod
    def random_name():
        return str(int(random()*1000000000))

    @staticmethod
    def read_spark_result(dir_name):
        result = glob.glob(os.path.join(dir_name, '*/part*'))[0]
        with open(result) as f:
            content = f.readlines()
        j = [json.loads(line) for line in content]
        return j

    @staticmethod
    def write_string(filename, input):
        with open(filename, 'w') as f:
            f.write(input)
