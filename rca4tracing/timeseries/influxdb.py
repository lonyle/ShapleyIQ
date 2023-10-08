from shutil import Error
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
from requests.models import Response
from rca4tracing.common.logger import setup_logger
from rca4tracing.common.utils import get_proj_dir, parse_yaml_file
import typing as t
import orjson
import yaml
import os
import time

LOG = setup_logger(__name__)



def quoted(word):
    return "\"" + word + "\""


class InfluxDB(object):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        proj_dir = get_proj_dir()
        # with open(os.path.join(proj_dir, 'conf/tsdb.yaml'), 'r', encoding='utf-8') as fr:
        #     tsdb_conf = yaml.load(fr, Loader=yaml.FullLoader)
        tsdb_conf_obj = parse_yaml_file(os.path.join(proj_dir, 'conf/tsdb.yaml'))
        tsdb_conf = tsdb_conf_obj['TEST']
        self.env= os.environ.get('RCA_ENV', 'TEST')
        try:
            tsdb_conf = tsdb_conf_obj[self.env]
        except Exception as e:
            LOG.error(str(e))
        self.url = kwargs.get('url', tsdb_conf['url'])
        self.username = kwargs.get('username', tsdb_conf['username'])
        self.password = kwargs.get('password', tsdb_conf['password'])

    def request(self, api, data=None, headers=None, params=None):
        response = None
        verify = True
        if self.env == 'OXS': #in OXS, https is disabled
            verify = False
            
        try:
            response = requests.post(self.url + api,
                                     data=data,
                                     headers=headers,
                                     params=params,
                                     verify=verify)
            LOG.debug(response.text)
        except Exception as e:
            LOG.debug(e)
        return response

    '''
    transform tsdb query to influxdb query
    '''

    def tsdb_query(self, dbname, query):
        field_str = ''
        for field in query['fields']:
            field_str += field['field'] + ','
        field_str = field_str[:-1]

        tag_str = ''
        if 'tags' in query:
            for tag_key in query['tags']:
                tag_str += '{}={} AND '.format(tag_key, query['tags'][tag_key])
            tag_str = tag_str[:-5]
            tag_str += ' AND'
        q = 'SELECT {} FROM {} WHERE {} time > {}s'.format(\
            field_str, query['metric'], tag_str, query['start'])
        if query['end'] is not None:
            q += ' AND time < {}s'.format(query['end'])
        LOG.info(q)
        return self.query(dbname, data=q)

    """
    put data to tsdb
    """

    def put_list(self, 
                 dbname: str, 
                 data: t.List):
        """put a list of records into influxdb

        Args:
            dbname (str): [description]
            data (t.List): [description]

        Returns:
            [type]: [description]
        """        
        api = f"/write?db={dbname}&u={self.username}&p={self.password}"
        headers = {'Content-Type': 'application/octet-stream'}
        """data template
        'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
        """
        data_str = ''
        for _, span in enumerate(data):
            timestamp = span['timestamp']
            assert timestamp > 10**12, "Only 13digits timestamp is accepted"
            if timestamp < 10**13 and timestamp >= 10**12:
                timestamp = timestamp * (10**6)
            if timestamp < 10**16 and timestamp >= 10**13:
                timestamp = timestamp * (10**3)
            data_str = data_str + f"{span['metric']}"
            for key, value in span['tags'].items():
                data_str = data_str + f",{key}={value}"
            data_str = data_str + " "
            for i, item in enumerate(span['fields'].items()):
                if i == 0:
                    data_str = data_str + f"{item[0]}={item[1]}"
                else:
                    data_str = data_str + f",{item[0]}={item[1]}"
            data_str = data_str + f" {int(timestamp)}\n"

        LOG.debug(data_str)

        response = self.request(api, data_str, headers=headers)
        LOG.debug('response of put_list to influxdb: {}'.format(response.text))
        return response
    
    def put(self, 
            dbname: str, 
            measurements, 
            fields: t.Dict, 
            tags: t.Dict,
            timestamp: int):
        """[summary]

        Args:
            dbname (str): name of database
            measurements ([type]): name of metric
            fields (t.Dict): [description]
            tags (t.Dict): [description]
            timestamp (int): [description]

        Returns:
            [type]: [description]
        """
        api = f"/write?db={dbname}&u={self.username}&p={self.password}"
        headers = {'Content-Type': 'application/octet-stream'}
        """data template
        'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
        """
        assert timestamp > 10**12, "Only 13digits timestamp is accepted"
        if timestamp < 10**13 and timestamp >= 10**12:
            timestamp = timestamp * (10**6)
        if timestamp < 10**16 and timestamp >= 10**13:
            timestamp = timestamp * (10**3)

        data_str = f'{measurements}'
        for key, value in tags.items():
            data_str = data_str + f",{key}={value}"
        data_str = data_str + " "
        for i, item in enumerate(fields.items()):
            if i == 0:
                data_str = data_str + f"{item[0]}={item[1]}"
            else:
                data_str = data_str + f",{item[0]}={item[1]}"
        data_str = data_str + f" {timestamp}"
        return self.request(api, data_str, headers=headers)

    def query(self, dbname: str, data: str):
        #print (dbname)
        api = f"/query?db={dbname}&u={self.username}&p={self.password}"
        params = {'db': f"{dbname}", 'q': data}
        res = self.request(api, data, params=params)
        if res is None:
            LOG.debug('request result is None.')
            return None
        if res.status_code != 200:
            LOG.error(f'request status code is {res.status_code}: {res.json()}')
            return None
        res = orjson.loads(res.content)
        return res['results']

    def create_client(self, database="metric"):
        client = InfluxDBClient(host=self.__host,
                                port=8086,
                                username=self.__user,
                                password=self.__password,
                                database=database)
        return client

    def get_tag_values(self, dbname, key_name):
        q = f"show tag values with key={key_name}"
        res = self.query(dbname, q)
        return res

    def get_recent_tag_values(self, dbname, key_name):
        end_time = int(time.time())
        start_time = end_time - 30*3600*24 # recent one month
        # key_name = "ResultCode"
        # q = f"SELECT DISTINCT({quoted(key_name)}) FROM (select {quoted(key_name)},{quoted('ResultCode')} FROM {quoted('trace')} WHERE time > {start_time}s and time < {end_time}s)"
        q = f"show tag values with key={key_name} where time > {start_time}s"
        # print (q)
        res = self.query(dbname, q)
        # print (res)
        return res

    def drop_series(self, dbname, series_name):
        q = f"DELETE FROM {series_name}"
        res = self.query(dbname, q)
        return res
    
    def __repr__(self) -> str:
        return super().__repr__()
    
    def __str__(self) -> str:
        return f"""
                ENV:\t{self.env}
                URL:\t{self.url}
                USERNAME:\t{self.username}
                PASSWORD:\t{self.password}
                """


if __name__ == '__main__':
    test = InfluxDB()
    # test.drop_series('das_gateway', 'trace')
    # exit(0)
    dbname = 'metric'
    measurements = 'cpu_load_short'
    # tag value could be indexed
    tags = {'host': 'server02', 'region': "us-west"}
    fields = {'value': 100}
    # timestamp value is very strange
    timestamp = 143406558800
    q = "SELECT \"Duration\" FROM \"trace\" WHERE \"ServiceName\"='RDS_API'"
    q = "SELECT \"ResultCode\" FROM \"trace\" limit 10"
    # q="SHOW SERIES ON  \"trace\" WHERE time > now() - 1h"
    # res=test.put(dbname, measurements, fields, tags, timestamp)
    # print(res.content)
    # res=test.query(dbname, data=q)
    data = [
        {
            'metric': 'trace',
            'timestamp': 1434065588000,  #millisecond, no need to divide by 1000
            'fields': {
                'Duration': 10,
                'ResultCode': '200'
            },
            'tags': {
                'ServiceName': 'ServiceName',
                'OperationName': 'OperationName',
                'ResultCode': 'ResultCode'
            }
        },
        {
            'metric': 'trace',
            'timestamp': 1434065598000,  #millisecond, no need to divide by 1000
            'fields': {
                'Duration': 10,
                'ResultCode': '500'
            },
            'tags': {
                'ServiceName': 'ServiceName',
                'OperationName': 'OperationName',
                'ResultCode': 'ResultCode'
            }
        }
    ]

    import time
    data = [{
        "metric": "trace_anomaly",
        "timestamp": int(time.time() * 1000),
        "fields": {
            "value": 500,
            #"average": event['average'],
            "type": '\"QPS\"'
        },
        "tags": {
            'OperationName': 'test',
            'ServiceName': 'test',
            "reason": 'spike'
        }
    }]
    # printes)
    test.put_list('test', data)
