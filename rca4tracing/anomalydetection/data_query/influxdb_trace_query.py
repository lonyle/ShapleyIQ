'''
the lower-level apis for influxdb trace query
'''


import time
from datetime import datetime, tzinfo, timezone
from rca4tracing.timeseries.influxdb import InfluxDB 
import json

from rca4tracing.common.config_parser import AnomalyConfigParser
anomaly_cfg = AnomalyConfigParser(config_filename='anomaly_config.yaml').config

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='anomalydetection')

def datetime_str_to_timestamp(datetime_str):
    datetime_str = datetime_str.split('.')[0]
    datetime_str = datetime_str.split('Z')[0]
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    timestamp = datetime_obj.replace(tzinfo=timezone.utc).timestamp()
    return timestamp

class InfluxDBTraceQuery:
    ''' Trace Query get the raw source of data, and return a dictionary
    '''
    def __init__(self, return_result_code=False, dbname='dbpaas'):
        self.influxdb = InfluxDB()
        self.return_result_code = return_result_code
        self.dbname = dbname

    def get_duration_after(self, s_name, o_name, timestamp, debug=False):
        select_fields = "\"Duration\""
        if self.return_result_code:
            select_fields += ",\"ResultCode\""
        q = "SELECT " + select_fields + " FROM \"trace\" WHERE \"ServiceName\"='{}' \
            AND \"OperationName\"='{}' AND time > {}s" \
                .format(s_name, s_name+':'+o_name, timestamp+1)

        # print (q)
        res = self.influxdb.query(self.dbname, data=q)
        # print (res)
        return self.process_data(res, return_result_code=self.return_result_code)

    def get_duration_before_or_on(self, 
                                  s_name, 
                                  o_name, 
                                  timestamp, 
                                  look_back_seconds=3600*24*7, 
                                  aggregator=None,
                                  fill_zero=False,
                                  filter=''):
        start_timestamp = timestamp - look_back_seconds
        return_result_code = False
        if aggregator is None:
            select_fields = "\"Duration\""
            if self.return_result_code: # only if we do not use aggregator, we return the result code
                select_fields += ",\"ResultCode\""
                return_result_code = True
        else:
            select_fields = aggregator + "(\"Duration\")"            
            
        q = "SELECT " + select_fields + " FROM \"trace\" WHERE \"ServiceName\"='{}' \
            AND \"OperationName\"='{}' AND time <= {}s AND time > {}s {}" \
                .format(s_name, s_name+':'+o_name, timestamp, start_timestamp, filter)

        if aggregator is not None:
            q += " group by time(1m)"
        if fill_zero == True:
            q += " fill(0)"

        LOG.debug (q)

        res = self.influxdb.query(self.dbname, data=q)
        return self.process_data(res, return_result_code=return_result_code)

    def process_data(self, data_vec, return_result_code=True):
        ''' this is old version to process data, it works if we have one field (e.g. one aggregator)
        '''
        values = []
        timestamps = []
        result_codes = []

        if data_vec is not None:
            for data in data_vec:
                if 'series' not in data:
                    LOG.debug(
                        "'series' is not in data, the data is: {}".format(data))
                    continue
                rows = data['series'][0]['values']
                for row in rows:
                    timestamp = datetime_str_to_timestamp(row[0])
                    timestamps.append(timestamp)
                    values.append(row[1])
                    if return_result_code:
                        result_codes.append(row[2])

        else:
            LOG.debug("The data is None")

        if return_result_code:
            return values, timestamps, result_codes
        else:
            return values, timestamps

    ####################################################################################
    # the following is added by guizhihao, with multiple aggregators
    ####################################################################################
    def get_duration_after_with_multi_aggregator(self, s_name, o_name, timestamp, debug=False, aggregator_dict = None):
        aggregator_list = None if aggregator_dict is None else list(aggregator_dict.values())

        if aggregator_list is None:
            select_fields = "\"Duration\""
            if self.return_result_code:  # only if we do not use aggregator, we return the result code
                select_fields += ",\"ResultCode\""
        else:
            select_fields = aggregator_list[0] + "(\"Duration\")"
            for it in aggregator_list[1:]:
                select_fields += ", " + it + "(\"Duration\")"

        # added by liye on 2021-11-15: for QPS, if time.time() - timestamp < min_time_diff_qps_seconds, we set the timestamp
        start_timestamp = timestamp+1 # default +1 because we want to always get the newest
        current_timestamp = int(time.time())
        if current_timestamp - start_timestamp <= anomaly_cfg['min_time_diff_qps_seconds']:
            LOG.debug("current_timestamp: {}, start_timestamp: {}, min_diff: {}".format(current_timestamp, start_timestamp, anomaly_cfg['min_time_diff_qps_seconds']))
            start_timestamp = current_timestamp - anomaly_cfg['min_time_diff_qps_seconds']

        q = "SELECT " + select_fields + " FROM \"trace\" WHERE \"ServiceName\"='{}' \
            AND \"OperationName\"='{}' AND time > {}s" \
                .format(s_name, s_name+':'+o_name, start_timestamp) 

        if aggregator_list:
            q += " group by time(1m)"

        # print (q)
        res = self.influxdb.query(self.dbname, data=q)
        # print (res)

        value_dict, timestamps = self.process_data_multi_aggregator(res,
            return_result_code=self.return_result_code, metric_list=list(aggregator_dict.keys()))
        return value_dict, timestamps


    def get_duration_before_or_on_with_multi_aggregator(self, s_name, o_name, timestamp, look_back_seconds=3600 * 24 * 7,
                                  aggregator_dict=None):
        ''' this function returns a value_dict (possibly including the result_codes)
        '''
        start_timestamp = timestamp - look_back_seconds
        return_result_code = False

        aggregator_list = None if aggregator_dict is None else list(aggregator_dict.values())

        if aggregator_list is None:
            select_fields = "\"Duration\""
            if self.return_result_code:  # only if we do not use aggregator, we return the result code
                select_fields += ",\"ResultCode\""
                return_result_code = True
        else:
            select_fields = aggregator_list[0] + "(\"Duration\")"
            for it in aggregator_list[1:]:
                select_fields += ", " + it + "(\"Duration\")"

        q = "SELECT " + select_fields + " FROM \"trace\" WHERE \"ServiceName\"='{}' \
            AND \"OperationName\"='{}' AND time <= {}s AND time > {}s" \
            .format(s_name, s_name + ':' + o_name, timestamp, start_timestamp)

        if aggregator_list:
            q += " group by time(1m)"

        LOG.debug(q)

        res = self.influxdb.query(self.dbname, data=q)

        if res is None:
            return {}, [] # return empty

        #如果该接口历史数据过多，会报超时错误，回查时间减半
        while 'error' in res[0]:
            LOG.error("KEY: {}:{}  Error Msg: {}".format(s_name, o_name, res[0]['error']))
            look_back_seconds = look_back_seconds//2
            start_timestamp = timestamp - look_back_seconds
            q = "SELECT " + select_fields + " FROM \"trace\" WHERE \"ServiceName\"='{}' \
                AND \"OperationName\"='{}' AND time <= {}s AND time > {}s" \
                .format(s_name, s_name + ':' + o_name, timestamp, start_timestamp)
            if aggregator_list:
                q += " group by time(1m)"
            res = self.influxdb.query(self.dbname, data=q)
            if res is None:
                return {}, [] # return empty

        value_dict, timestamps = self.process_data_multi_aggregator(res,
            return_result_code=return_result_code, metric_list=list(aggregator_dict.keys()))
        
        return value_dict, timestamps

    def process_data_multi_aggregator(self,
                                      data_vec,
                                      return_result_code=True,
                                      metric_list=[]):
        ''' this is the modified version by guzhihao, that can process data with multiple aggregator
        '''
        value_dict = dict()
        timestamps = []
        result_codes = []

        if data_vec is not None:
            for data in data_vec:
                if 'series' not in data:
                    LOG.debug ("'series' is not in data, the data is: {}".format(data))
                    continue
                rows = data['series'][0]['values']

                tmp = list(zip(*rows))
                timestamps = [int(datetime_str_to_timestamp(it)) for it in tmp[0]]

                idx = 1
                for metric in metric_list:
                    value_dict[metric] = tmp[idx]
                    idx += 1
                if return_result_code:
                    value_dict['result_codes'] = tmp[-1]

        else:
            LOG.debug("The data is None")

        # added on 2021-11-10: for QPS, if count is zero, we should add [0] to the output
        if len(timestamps) == 0:
            if 'QPS' in metric_list:
                value_dict['QPS'] = [0]

        # print (value_dict.keys())
        return value_dict, timestamps



    