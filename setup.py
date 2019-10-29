from sizeofweb import CountGraphNodesJob
from linkgraph import MRCalculateLinkGraph
from mainpage import DanglingNodeJob
from rankpage import PageRankJob
from initialpage import DistributeInitialPageRankJob

import time
import sys

output_dir = 's3://commoncrawl/crawl-data/CC-MAIN-2019-39'
input_file = sys.argv[1]
runner_type = 'emr'
emr_args = ['--cluster-id', 'j-<cluster-nr>', '--conf-path', 'mrjob.conf']

current_time = time.strftime("%Y%m%d-%H%M%S")
job1 = MRCalculateLinkGraph(['-r', runner_type, input_file,
    '--output', output_dir + '-linkgraph-' + current_time] + emr_args)
job1.set_up_logging()
with job1.make_runner() as runner1:
    runner1.run()
    link_graph_result = runner1.get_output_dir()

    job2 = CountGraphNodesJob(['-r', runner1.alias, link_graph_result] + emr_args)
    job2.set_up_logging()
    with job2.make_runner() as runner2: 
        runner2.run()
        secondOutput = runner2.get_output_dir()
        graph_size = int(list(runner2.stream_output())[0].strip())

    job4 = DistributeInitialPageRankJob(['-r', runner1.alias, link_graph_result, '--graph-size', str(graph_size)] + emr_args)
    job4.set_up_logging()
    with job4.make_runner() as runner4:
        runner4.run()
        current_graph = runner4.get_output_dir()

        max_iterations = 15 # I read normally you want like 30 iterations, but we only have a part of the whole data
        for i in range(max_iterations):
            job5 = DanglingNodeJob(['-r', runner1.alias, current_graph] + emr_args)
            job5.set_up_logging()
            with job5.make_runner() as runner5:
                runner5.run()
                dangling_node_pr = float(list(runner5.stream_output())[0].strip())
            
            current_time = time.strftime("%Y%m%d-%H%M%S")
            args = ['-r', runner1.alias, current_graph,
                    '--graph-size', str(graph_size),
                    '--dangling-node-pr', str(dangling_node_pr),
                    '--damping-factor', str(0.85)] + emr_args

            if i == max_iterations - 1: # last iteration
                args += ['--output', output_dir + '-' + str(i) + '-' + current_time]

            job6 = PageRankJob(args)
            job6.set_up_logging()
            with job6.make_runner() as runner6:
                runner6.run()
                print(runner6.get_output_dir())