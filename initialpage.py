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

class DistributeInitialPageRankJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def configure_options(self):
        super(DistributeInitialPageRankJob, self).configure_options()
        self.add_passthrough_option('--graph-size', dest='size_of_web', type='int', default=0) 

    def mapper(self, website, node):
        node['state'] = 1.0 / self.options.size_of_web
        yield website, node


if __name__ == '__main__':
    DistributeInitialPageRankJob().run()