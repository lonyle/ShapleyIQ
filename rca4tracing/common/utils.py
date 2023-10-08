from contextlib import contextmanager
from re import L
from rca4tracing.common.logger import setup_logger
import os
import shutil
import requests
import hashlib
import hmac
import six
import base64
try:
   import cPickle as pickle
except:
   import pickle


LOG = setup_logger(__name__)
import time


@contextmanager
def timing_context(name):
    startTime = time.process_time()
    yield
    elapsedTime = time.process_time() - startTime
    LOG.info('[{}] finished in {} ms'.format(name, int(elapsedTime * 1000)))


def get_proj_dir():
    import os
    from pathlib import Path
    return Path(os.path.dirname(os.path.realpath(__file__))).parent.parent


def get_file_list(folder):
    from os import listdir
    from os.path import isfile, join
    return (join(folder, f) for f in listdir(folder) if isfile(join(folder, f)))


def split_n_list(seq, n):
    res = []
    if len(seq) < n:
        res = [[i] for i in seq]
        i = len(seq)
        while i < n:
            res.append([])
            i = i + 1
        return res
    avg = len(seq) / float(n)
    last = 0.0
    while last < len(seq):
        res.append(seq[int(last):int(last + avg)])
        last += avg
    return res


def reduce_dict_list(trace_list):
    result_dict = {}
    for id, trace in enumerate(trace_list):
        if id == 0:
            result_dict = trace.copy()
        else:
            for key in trace.keys():
                vals = result_dict.get(key, [])
                # vals.append(trace[key])
                vals.extend(trace[key])
                result_dict[key] = vals
    return result_dict


def split_dict(input_dict, num):
    res = list(input_dict.values())
    return split_n_list(res, num)


def folder_check_remove(data_dir):
    if os.path.exists(data_dir):
        try:
            shutil.rmtree(data_dir)
        except OSError as e:
            LOG.error("data file creation", (e.filename), (e.strerror))


def parse_yaml_file(file_name):
    import yaml
    with open(file_name, 'r', encoding='utf-8') as stream:
        try:
            yaml_object = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return yaml_object

def url_request(url, params, retris=0):
    r = None
    try:
        r = requests.get(url, params)
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as errh:
        LOG.error("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        LOG.error("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        LOG.error("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        LOG.error("OOps: Something Else",err)
    finally:
        return r

def url_request_post(url, params, retries=0):
    r = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
    try:
        r = requests.post(url, params, headers=headers)
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as errh:
        LOG.error("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        LOG.error("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        LOG.error("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        LOG.error("OOps: Something Else",err)
    finally:
        return r

class AverageMeter(object):

    def __init__(self):
        self.reset()

    def is_empty(self):
        return self.cnt == 0

    def reset(self):
        self.avg = 0.
        self.sum = 0.
        self.cnt = 0

    def update(self, val, n=1):
        self.sum += val * n
        self.cnt += n
        self.avg = self.sum / self.cnt

def compress_data(data):
        import zlib
        return zlib.compress(data, 6)

def cal_md5(content):
    from typing import List
    if isinstance(content, List):
        p = pickle.dumps(content, -1)
        return hashlib.md5(p).hexdigest()
    return hashlib.md5(content).hexdigest().upper()

def base64_encodestring(s):
    if six.PY2:
        return base64.encodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.encodebytes(s).decode('utf8')


def base64_decodestring(s):
    if six.PY2:
        return base64.decodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.decodebytes(s).decode('utf8')

def hmac_sha1(content, key):
    import six
    if isinstance(content, six.text_type):  # hmac.new accept 8-bit str
        content = content.encode('utf-8')
    if isinstance(key, six.text_type):  # hmac.new accept 8-bit str
        key = key.encode('utf-8')

    hashed = hmac.new(key, content, hashlib.sha1).digest()
    return base64_encodestring(hashed).rstrip()

