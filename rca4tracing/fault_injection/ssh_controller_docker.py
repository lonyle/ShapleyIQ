import json
import time
import subprocess

import rca4tracing.fault_injection.config as fi_cfg

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class SshControlerDocker:
    def __init__(self, 
                 # remote_prefix='ssh root@8.136.136.64'
                 remote_prefix=None):
        # self.remote_prefix = remote_prefix
        if remote_prefix is not None:
            self.remote_prefix = remote_prefix
        else:            
            self.remote_prefix = fi_cfg.docker_remote_prefix

    def get_container_id_for_name(self, name):
        ''' get container id for some service name
        '''
        remote_prefix_params = self.remote_prefix.split(' ')
        try:
            output = subprocess.check_output(remote_prefix_params+['docker', 'ps', '|', 'grep', name])
        except Exception as e:
            LOG.error(f'please check whether your name is valid, error: {str(e)}')

        output = output.decode('utf-8')
        container_id = output.split(' ')[0] # the first parameter is the container id
        local_port = output.split('->')[1].split('/')[0]

        return container_id, local_port

    def exec_network_delay(self, container_id, local_port, delay_in_ms):
        start_time = time.time()
        remote_prefix_params = self.remote_prefix.split(' ')
        output = subprocess.check_output(remote_prefix_params+['blade', 'create', 'docker', 'network', 'delay',
            '--time', str(delay_in_ms), '--interface', 'eth0', '--container-id', container_id,
            '--local-port', local_port])#, '--force'])
        ret = json.loads(output.decode('utf-8'))
        LOG.info(f"time for exec_network_delay: {time.time()-start_time}")
        if ret["code"] != 200:
            LOG.error(f"returned with error: {ret}")
            return None
        else:
            LOG.info(ret)
            fault_id = ret["result"]
            return fault_id

    def exec_destroy_fault(self, fault_id):
        start_time = time.time()
        remote_prefix_params = self.remote_prefix.split(' ')
        output = subprocess.check_output(remote_prefix_params+['blade', 'destroy', fault_id])
        ret = json.loads(output.decode('utf-8'))
        LOG.info(f"time for exec_destroy_fault: {time.time()-start_time}")
        if ret["code"] != 200:
            LOG.error(f"returned with error: {ret}")
        else:
            LOG.info(ret)

