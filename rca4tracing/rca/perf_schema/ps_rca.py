import json 
import numpy as np 

from rca4tracing.rca.perf_schema.converter import ps2trace, trace2jaeger
from rca4tracing.datasources.perf_schema.driver import PerfSchemaDriver# , PerfSchemaData, PerfSchemaEventNode

from rca4tracing.rca.shapley_value_rca import ShapleyValueRCA

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class PerformanceSchemaRCA:
    def __init__(self):
        self.jaeger_trace_folder = 'rca4tracing/datasources/tests/data/'
    
    def find_sql_for_node(self, perf_schema_data, node_id):
        node_type = node_id.split(':')[0]
        node = perf_schema_data.nodes[node_type][node_id]
        while node_type != 'Statement':
            if node_type == 'Transaction':
                nodes = node.children
                return [nodes[node_id].data['SQL_TEXT'] for node_id in nodes]
            else:
                node = node.parent
            node_type = node.node_type 
        return node.data['SQL_TEXT']

    def analyze(self, 
                perf_schema_data, 
                target_node_ids=None,
                dump_to_jaeger=False,
                jaeger_prefix='',
                strategy='avg_by_contribution',
                fault_type='others'):
        ''' 
            Args:
              target_node_ids: [dict], e.g. {'id1': 'Transaction'}
                if target_node_ids is None, we get all the transaction_node and statement_node
            
        '''
        if target_node_ids is None:
            target_node_ids = dict()
            ps_driver = PerfSchemaDriver()
            ps_driver.set_perf_schema_data(perf_schema_data)
            transaction_node_ids, statement_node_ids = ps_driver.get_analyze_nodes(fault_type=fault_type)
            for transaction_node_id in transaction_node_ids:
                target_node_ids[transaction_node_id] = 'Transaction'
            for statement_node_id in statement_node_ids:
                target_node_ids[statement_node_id] = 'Statement'

        traces = []
        for event_node_id in target_node_ids:
            event_type = target_node_ids[event_node_id]
            trace = ps2trace(perf_schema_data, event_node_id, event_type=event_type)
            traces.append(trace)

            if dump_to_jaeger == True:
                jaeger_trace = trace2jaeger(trace, trace_id=event_node_id)
                jaeger_filename = self.jaeger_trace_folder + jaeger_prefix + event_node_id + '.json'
                with open(jaeger_filename, 'w') as out_f:
                    json.dump(jaeger_trace, out_f, indent=4)
            
        # # print (trace)
        shapley_rca = ShapleyValueRCA(using_cache=False)
        # strategy = 'avg_by_contribution'
        # strategy = 'avg_by_prob'
        ret = shapley_rca.analyze_traces(traces, strategy=strategy, sort_result=True)    
        LOG.info ("result of ShapleyRCA: {}".format(dict(list(ret.items())[:5])))

        sqls = []
        for node_key in ret:
            # print (node_key)
            node_id = node_key.replace('::', '_').split(':', 1)[1]
            sql = self.find_sql_for_node(perf_schema_data, node_id)
            sqls.append(sql)
        
        return sqls

    def analyze_wait_events(self, perf_schema_data):
        ''' for cpu and network problem
        '''
        wait_time_stat = dict()
        for wait_node_id in perf_schema_data.nodes['Wait']:
            wait_node = perf_schema_data.nodes['Wait'][wait_node_id]
            data = wait_node.data
            if data['EVENT_NAME'] not in wait_time_stat:
                wait_time_stat[ data['EVENT_NAME'] ] = []
            if data['TIMER_WAIT']:
                wait_time_stat[ data['EVENT_NAME'] ].append(data['TIMER_WAIT'])

        wait_time_sum = dict()
        for event_name in wait_time_stat:
            wait_time_sum[event_name] = np.average(wait_time_stat[event_name])
        
        return dict(sorted(wait_time_sum.items(), key=lambda x: x[1], reverse=True))
        

    def print_transactions(self, transaction_node_ids):
        for transaction_node_id in transaction_node_ids:
            sqls = self.find_sql_for_node(perf_schema_data, transaction_node_id)
            print (sqls)

        print(transaction_node_ids)

    
