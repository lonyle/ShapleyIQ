# the base class defining the needed data

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class RCAData():
    def __init__(self, root_causes, input_path, metric_list=['MaxDuration', 'Duration']):
        # the data to prepare
        # 1. from trace data
        self.edges = []
        self.nodes_id = []
        self.root_id = None
        self.root_id_list = [] # for operation-level analysis, more than one root_id will be affected
        self.trace_data_dict = dict() # in yipei's format, for one trace. the key is the node_id
        self.request_timestamp = None # int

        # 2. from metric data derived from trace
        self.ts_data_dict = dict()
        self.metrics_statistical_data = dict()
        self.metrics_threshold = dict()

        # 3. from machine metrics
        self.operation_ip_dict = dict()
        self.ip_ts_data_dict = dict() # machine data

        # the following is used by MicroRank
        self.normal_trace_dict = dict()
        self.abnormal_trace_dict = dict()
        #####################

        # the following is for operation-level or global-level
        # for operation-level or global-level, the edges and nodes_id will be merged
        self.traces = None
        #####################
        
        self.root_causes = root_causes # the true root_causes. NOTE that there might be multiple root causes
        self.metric_list = metric_list # metric names of time series
        self.input_path = input_path

    def get_operation_ip_dict(self, ip_mapping=None):
        ''' get ips for all operations

            Params
            -------
            if ip_mapping is not None, then we translate the ip according to the mapping
        '''
        operation_ip_dict = {}
        for node_id in self.nodes_id:
            ip = self.trace_data_dict[node_id]['serverIp'][0]
            if ip_mapping is not None:
                if ip in ip_mapping:
                    ip = ip_mapping[ip]
                else:
                    LOG.warning(f"ip {ip} is not in the ip_mapping")
            operation_ip_dict[node_id] = ip
        
        return operation_ip_dict




    