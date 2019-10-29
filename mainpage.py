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

class PageRankJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def configure_options(self):
        super(PageRankJob, self).configure_options()
        self.add_passthrough_option('--graph-size', dest='size_of_web', type='int', default=0) 
        self.add_passthrough_option('--dangling-node-pr', dest='dangling_node_pr', type='float', default=0) 
        self.add_passthrough_option('--damping-factor', dest='damping_factor', type='float', default=0.85) 

    def mapper(self, website, node):
        yield website, ('node', node)

        for outgoing in node['outgoing']:
            msg = node['state'] / len(node['outgoing'])
            yield outgoing, ('msg', msg)

    def reducer(self, website, data):
        node = None
        msgs = []

        for msg_type, msg_val in data:
            if msg_type == 'node':
                node = msg_val
            elif msg_type == 'msg':
                msgs.append(msg_val)

        if node != None:
            node['state'] = self.options.damping_factor * sum(msgs) \
                    + self.options.damping_factor * self.options.dangling_node_pr / self.options.size_of_web \
                    + (1 - self.options.damping_factor) / self.options.size_of_web
            yield website, node


if __name__ == '__main__':
    PageRankJob().run()