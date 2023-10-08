import os
import json
import time

from rca4tracing.rca.experiment.rca_data import RCAData

from rca4tracing.rca.baselines.MicroHECL import MicroHECL
from rca4tracing.rca.baselines.MicroRCA import MicroRCA
from rca4tracing.rca.baselines.TON import TON
from rca4tracing.rca.baselines.MicroRank import MicroRank
from rca4tracing.rca.utils import contribution_to_prob
from rca4tracing.rca.shapley_value_rca import ShapleyValueRCA

from rca4tracing.rca.experiment.get_rca_data_jaeger import RCADataJaeger

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class RunRCA:
    def __init__(self, 
                 input_path, 
                 output_path,
                 root_causes=None, # it is a list, consider the case with multiple root causes
                 trace_id=None, # single trace_id mode
                 timestamp=None, # used together with look_back_seconds
                 look_back_seconds=None, # specify services
                 anomaly_conditions=None, # specify services. services and threshold for abnormal RT. for jaeger's case, we rely on this to label the root cause
                 data_source='dbaas',
                 ip_mapping=None):
        # super().__init__(trace_id, root_cause, input_path)
        self.output_path = output_path
        self.data_source = data_source
        if data_source == 'dbaas':
            if trace_id is None:
                LOG.warning('trace_id is None for data source: dbaas')
            from rca4tracing.rca.experiment.get_rca_data_dbaas import RCADataDBaaS
            self.data = RCADataDBaaS(input_path, trace_id=trace_id, root_causes=root_causes)
            
        elif data_source == 'jaeger':
            if (look_back_seconds is None or anomaly_conditions is None) and ():
                LOG.warning('for data source jaeger, you should either')
            
            self.data = RCADataJaeger(input_path, trace_id=trace_id, root_causes=root_causes,
                                      timestamp=timestamp,
                                      look_back_seconds=90,
                                      anomaly_conditions=anomaly_conditions,
                                      ip_mapping=ip_mapping)
        ret = self.data.load_all_data()
        if isinstance(ret, RCADataJaeger): # load the data from ret
            self.data = ret

        self.empty_data = (ret is None) # if empty, we will not run the algorithms

    
    def run(self, algorithm_name, params={}):
        # the entry point to run some algorithm   
        if self.empty_data:
            LOG.warning('the data is empty, skip')
            return -1
        func = getattr(self, 'run_'+algorithm_name)
        start_time = time.time()
        result = func(**params)
        running_time = time.time() - start_time
        return result, running_time
        


    def run_MicroHECL(self, save_result=True):
        microhecl = MicroHECL(self.data.edges, 
                              self.data.nodes_id, 
                              self.data.trace_data_dict, 
                              self.data.request_timestamp)
        microhecl.get_data(self.data.ts_data_dict, 
                           self.data.metrics_statistical_data, 
                           self.data.metrics_threshold)
        result = microhecl.run(initial_anomalous_node=self.data.root_id, detect_metrics=['RT'])
        
        if save_result:
            self.save_result(result, 'microhecl.json')

        return result

    def run_MicroRCA(self, save_result=True, operation_only=False):
        microrca = MicroRCA(self.data.edges, 
                            self.data.nodes_id, 
                            self.data.trace_data_dict, 
                            self.data.request_timestamp)
        microrca.get_data(self.data.ts_data_dict, 
                          self.data.metrics_statistical_data, 
                          self.data.metrics_threshold, 
                          self.data.ip_ts_data_dict)
        result = microrca.run()

        if save_result:
            self.save_result(result, 'microrca.json')

        if operation_only:
            operation_score = {}
            for key, value in result.items():
                if key in self.data.nodes_id:
                    operation_score[key] = value
            return operation_score

        return result

    def run_TON(self, save_result=True, operation_only=False):
        if self.data_source == 'jaeger':
            ip_mapping = self.data.ip_mapping
        else:
            ip_mapping = None
        ton = TON(self.data.edges, 
                  self.data.nodes_id, 
                  self.data.root_id, # we also need the root_id
                  self.data.trace_data_dict, 
                  self.data.request_timestamp,
                  ip_mapping=ip_mapping)
        # only use the time series data
        ton.get_data(self.data.ts_data_dict, self.data.ip_ts_data_dict)
        result = ton.run()

        if save_result:
            self.save_result(result, 'ton.json')

        if operation_only:
            operation_score = {}
            for key, value in result.items():
                if key in self.data.nodes_id:
                    operation_score[key] = value
            return operation_score

        return result

    def run_MicroRank(self, save_result=True, n_sigma=3):
        microrank = MicroRank(self.data.root_id, 
                              self.data.request_timestamp,
                              n_sigma=n_sigma)
        # only need the normal and abnormal traces
        microrank.get_data(self.data.trace_dict, # trace dict 
                           self.data.metrics_statistical_data)
        result = microrank.run()
        
        if save_result:
            self.save_result(result, 'microrank.json')

        return result

    def run_ShapleyValueRCA(self, 
                            save_result=True, 
                            using_cache=False, 
                            multiple_metric=False, 
                            strategy='avg_by_contribution'):
        # using_cache = True if (self.data_source == 'dbaas') else False
        svrca =  ShapleyValueRCA(using_cache=using_cache)
        if self.data.traces is None:
            traces = [self.data.spans] # backward compatibility
        else:
            traces = self.data.traces
        
        # start_time = time.time()
        adjusted_contribution_dict = svrca.analyze_traces(traces, strategy=strategy)
        # print (f"running_time: {time.time()-start_time}")
        result = contribution_to_prob(adjusted_contribution_dict)
        result = dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

        if self.data_source == 'jaeger' and multiple_metric == True:
            self.run_multiple_metric_rca(result)

        if save_result:
            self.save_result(result, 'shapleyvaluerca.json')

        return result

    def run_multiple_metric_rca(self, 
                                result, 
                                top_k=3, 
                                prob_threshold=0.05,
                                target_metric='MaxDuration'):
        from rca4tracing.rca.multiple_metric_rca.linear_multiple_metric_rca import LinearMultipleMetricRCA
        # similar to rca4tracing/rca/single_node_shapley_rca.py
        for node_id in list(result.keys())[:top_k]:
            if result[node_id] < prob_threshold:
                continue
            multi_metric_rca = LinearMultipleMetricRCA()

            ip = self.data.trace_data_dict[node_id]['serverIp'][0] # the same as rca_data.py
            if self.data.ip_mapping is not None:
                ip = self.data.ip_mapping[ip]

            time_series = self.data.ip_ts_data_dict[ip]
            time_series[target_metric] = self.data.ts_data_dict[node_id][target_metric]
            LOG.info (f"time_series: {time_series}")
            contribution_dict = multi_metric_rca.analyze(
                target_difference=1,
                target_metric_name=target_metric,
                time_series=time_series,
                return_prob=True
            )
            LOG.info(contribution_dict)

    def run_CauseInfer(self, save_result=True):
        from rca4tracing.rca.baselines.CauseInfer import CauseInfer # import here to save init time
        # convert the cause infer data 
        trace_id = self.data.trace_id
        max_lag = 2
        if self.data_source == 'jaeger':
            cause_infer = CauseInfer(trace_id,
                                    data_source=None,
                                    max_lag=3)
            cause_infer.set_data(self.data, data_source='jaeger')
        else:
            cause_infer = CauseInfer(trace_id,
                                    data_source='dbaas',
                                    max_lag=8)

        try:
            result = cause_infer.analyze()
        except Exception as e:
            result = {}
            LOG.error("Exception in cause_infer.analyze(): {}".format(str(e)))

        if save_result:
            self.save_result(result, 'cause_infer.json')

        return result


    def save_result(self, result, file_name):
        path = self.output_path + '/results/{}/'.format(self.data.trace_id)
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)
        with open(path + file_name, 'w')as f:
            f.write(json.dumps(result, indent=4))

