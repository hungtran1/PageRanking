import urlparse
import ujson as json
import sys
import time
import itertools
import getopt

from shutil import copyfile

import boto
import warc

from boto.s3.key import Key
from gzip import GzipFile
from mrjob.job import MRJob
from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol

class DanglingNodeJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def mapper(self, website, node):
        if len(node['outgoing']) == 0:
            yield '_', str(node['state'])
        else:
            yield '_', str(0)

    def reducer(self, _, states):
        yield '_', str(sum(list(map(float, states))))

if __name__ == '__main__':
    DanglingNodeJob().run()