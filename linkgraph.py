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

class MRCalculateLinkGraph(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    INPUT_PROTOCOL = RawValueProtocol

    def map_linkgraph(self, _, commoncrawl_file):
        if '\t' in commoncrawl_file:
            commoncrawl_file = commoncrawl_file.split('\t')[1]

        try:
            conn = boto.connect_s3(anon=True)
            bucket = conn.get_bucket('aws-publicdatasets', validate=True)
            key = Key(bucket, commoncrawl_file)

            import tempfile
            fp = tempfile.NamedTemporaryFile('w+b')
            content = key.read(4096)
            while content:
                fp.write(content)
                content = key.read(4096)
            fp.seek(0)

            f = warc.WARCFile(fileobj=GzipFile(fileobj=fp, mode='rb'))
            for record in f:
                if record['content-type'] != 'application/json':
                    continue

                payload = record.payload.read()

                try:
                    page_info = json.loads(payload)
                    page_url = page_info['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
                    page_domain = urlparse.urlparse(page_url).netloc
                    links = page_info['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
                    domains = set(filter(None, [urlparse.urlparse(url['url']).netloc for url in links]))
        
                    if len(domains) > 0:
                        yield page_domain, list(domains)
                except (KeyError, UnicodeDecodeError):
                    pass

            f.close()
        except:
            pass

    def reduce_linkgraph(self, domain, links):
        links = list(set(itertools.chain(*links)))
        yield domain, {'id': domain, 'state': 0, 'outgoing': links}

    def map_normalize(self, domain, data):
        yield data['id'], data

        for outgoing in data['outgoing']:
            yield outgoing, None

    def reduce_normalize(self, domain, data):
        self.increment_counter('linkgraph', 'size', 1)
        try:
            node = list(filter(lambda x: x is not None, data))[0]
        except IndexError:
            node = {'id': domain, 'state': 0, 'outgoing': []}
        
        yield domain, node

    def steps(self):
        return [MRStep(mapper=self.map_linkgraph, reducer=self.reduce_linkgraph),
                MRStep(mapper=self.map_normalize, reducer=self.reduce_normalize)]

if __name__ == '__main__':
    MRCalculateLinkGraph().run()