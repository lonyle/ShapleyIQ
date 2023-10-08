import pickle

from rca4tracing.datasources.perf_schema.driver import PerfSchemaDriver
from rca4tracing.rca.perf_schema.ps_rca import PerformanceSchemaRCA

def test_lock():
    fn = 'rca4tracing/datasources/tests/tpcc_lock_perf3.pickle'
    with open(fn, 'rb') as f:
        perf_schema_data = pickle.load(f) 
    ps_rca = PerformanceSchemaRCA()

    # target_node_ids = {'Transaction:ThreadID:135671-EventID:15410279': 'Transaction'}
    target_node_ids = None
    ps_rca.analyze(perf_schema_data, 
                   target_node_ids=target_node_ids,
                   dump_to_jaeger=True)


def test_io():
    fn = 'rca4tracing/datasources/tests/tpcc_io_perf2.pickle'
    with open(fn, 'rb') as f:
        perf_schema_data = pickle.load(f) 
    ps_rca = PerformanceSchemaRCA()

    # target_node_ids = {'Transaction:ThreadID:135671-EventID:15410279': 'Transaction'}
    target_node_ids = None
    ps_rca.analyze(perf_schema_data, 
                   target_node_ids=target_node_ids,
                   dump_to_jaeger=True,
                   jaeger_prefix='io_')

if __name__ == '__main__':
    # test_lock()
    test_io()