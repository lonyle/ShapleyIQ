import pickle
import json
import glob

from rca4tracing.datasources.perf_schema.driver import PerfSchemaData, PerfSchemaEventNode
from rca4tracing.rca.perf_schema.ps_rca import PerformanceSchemaRCA

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class Evaluator:
    def __init__(self, top_k_list=[1,2,3]):
        self.top_k_list = top_k_list 
        self.top_k_count = dict()
        for k in top_k_list:
            self.top_k_count[str(k)] = 0
        self.count = 0

    def get_top_k_ratio(self):
        top_k_ratio = dict()
        for key in self.top_k_count:
            if self.count != 0:
                top_k_ratio[key] = self.top_k_count[key] / self.count
            else:
                LOG.debug("number of cases is 0")
                top_k_ratio[key] = 0

        return top_k_ratio
        
    def check_result(self, rca_result, root_cause, root_cause_sql):
        LOG.debug(f"true_root_cause: {root_cause_sql}, rca_result: {rca_result}")
        if type(rca_result) is list: # transaction
            return root_cause_sql in rca_result
        else:
            return root_cause_sql == rca_result

    def dedup_results(self, rca_results):
        result_set = set()
        refined_results = []
        for rca_result in rca_results:
            rca_result_json = json.dumps(rca_result)
            if rca_result_json not in result_set:
                result_set.add(rca_result_json)
                refined_results.append(rca_result)
        return refined_results

    def evaluate(self, rca_results, root_cause, root_cause_sql):
        rca_results = self.dedup_results(rca_results)

        self.count += 1
        for k in self.top_k_list:
            for i in range(k):
                if i >= len(rca_results):
                    continue # less than i results
                if self.check_result(rca_results[i], root_cause, root_cause_sql):
                    self.top_k_count[str(k)] += 1
                    break

        # LOG.debug (f"count: {self.count}, top_k_ratio: {self.get_top_k_ratio()}")

def run_for_one_file(filename, evaluator, target_node_ids=None, fault_type='others'):
    with open(filename, 'rb') as f:
        perf_schema_data = pickle.load(f) 
    ps_rca = PerformanceSchemaRCA()

    short_filename = filename.split('/')[-1]

    # target_node_ids = None
    rca_results = ps_rca.analyze(perf_schema_data, 
                   target_node_ids=target_node_ids,
                   dump_to_jaeger=True,
                   strategy='avg_by_contribution',
                   jaeger_prefix=f"{short_filename}.",
                   fault_type=fault_type)

    # ret = ps_rca.analyze_wait_events(perf_schema_data)
    # LOG.info (ret)

    LOG.info("top 3 SQL:")
    for sql in rca_results[:3]:
        LOG.info (sql)
    
    # # print (sqls)
    # print (perf_schema_data.root_cause)
    print ("true root_cause_sql: {}".format(perf_schema_data.root_cause_sql))
    evaluator.evaluate(rca_results, 
                       perf_schema_data.root_cause, 
                       perf_schema_data.root_cause_sql)
    

def run_for_all(dataset_dir, pattern='*', fault_type='others'):
    filenames = glob.glob(f"{dataset_dir}{pattern}")
    evaluator = Evaluator()
    for fn in filenames:
        LOG.info(f"************************* filename: {fn} *************************")
        run_for_one_file(fn, evaluator, fault_type=fault_type)

if __name__ == '__main__':
    # filename = 'rca4tracing/datasources/perf_schema/datasets/seats_lock1.pickle'
    # evaluator = Evaluator()
    # target_node_ids = {'Transaction:ThreadID:2034-EventID:19871587': 'Transaction'}
    # run_for_one_file(filename, evaluator, target_node_ids)

    dataset_dir = 'rca4tracing/datasources/perf_schema/datasets/'
    pattern = 'tpcc_lock*'
    # pattern = 'tpcc_network100.*'
    # pattern = 'tpcc_mix1*'
    # pattern = 'tpcc_lock5_thread1_sleep1*'
    # pattern = 'smallbank_lock2*'
    run_for_all(dataset_dir, pattern=pattern, fault_type='others')