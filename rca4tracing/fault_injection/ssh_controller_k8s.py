''' this file is similar to ssh_controller_docker, we do some copy and paste
'''

import json
import time
import subprocess

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class SshControlerK8s:
    def __init__(self, 
                 remote_prefix='ssh root@118.31.76.75',
                 blade_bin='/root/chaosblade/target/chaosblade-1.5.0/blade'
                ):
        self.remote_prefix = remote_prefix
        self.remote_prefix_params = self.remote_prefix.split(' ')
        self.blade_bin = blade_bin

    def get_ip_mapping(self):
        ''' for k8s env, the ip recorded in jaeger is the pod's ip
            the mapping from innner_ip to external_ip
        '''
        node2external_ip = {
            'node1': '172.20.82.226', 
            'node2': '172.20.82.227', 
            'master': '172.20.82.224'
        }
        ip_mapping = dict()
        
        output = subprocess.check_output(self.remote_prefix_params + \
            ['kubectl', 'get', 'pods', '-o', 'wide'])

        output = output.decode('utf-8')
        if output:
            lines = output.split('\n')
            for line in lines[1:]:
                item = line.split()
                if len(item) > 4:
                    inner_ip, node_name = item[-4], item[-3]
                    ip_mapping[inner_ip] = node2external_ip[node_name]

        return ip_mapping
    
    def get_container_id_for_name(self, name):
        ''' get container id for some service name
            NOTE: we have node 1 and node 2, we should check which node has this service
        '''
        remote_prefix_params = self.remote_prefix_params

        self.node_name = None

        for node_name in ['node1', 'node2']:            
            try:
                output = subprocess.check_output(remote_prefix_params + ['ssh', node_name] \
                        +['docker', 'ps', '|', 'grep', name])
                output = output.decode('utf-8')
                if output:
                    container_id = output.split(' ')[0] # the first parameter is the container id
                    # local_port = output.split('->')[1].split('/')[0]
                    self.node_name = node_name 
                    
                    port_output = subprocess.check_output(remote_prefix_params + \
                        ['kubectl', 'get', 'services', '|', 'grep', name])
                    port_output = port_output.decode('utf-8')
                    print (port_output)
                    local_port = port_output.split()[-2].split('/')[0]

                    return container_id, local_port

            except Exception as e:
                LOG.info(f'the serivce {name} may not be on {node_name}, error: {str(e)}')
             

    def exec_network_delay(self, container_id, local_port, delay_in_ms):
        start_time = time.time()
        remote_prefix_params = self.remote_prefix_params
        # we exclude the local port to avoid the delay on the caller, only 
        output = subprocess.check_output(remote_prefix_params + ['ssh', self.node_name] \
            +[self.blade_bin, 'create', 'docker', 'network', 'delay',
                '--time', str(delay_in_ms), '--interface', 'eth0', '--container-id', container_id,
                '--exclude-port', local_port, 
                '--force'])

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
        remote_prefix_params = self.remote_prefix_params
        p = subprocess.Popen(remote_prefix_params + ['ssh', self.node_name] \
            +[self.blade_bin, 'destroy', fault_id])
        # ret = json.loads(output.decode('utf-8'))
        LOG.info(f"time for exec_destroy_fault: {time.time()-start_time}")
        # if ret["code"] != 200:
        #     LOG.error(f"returned with error: {ret}")
        # else:
        #     LOG.info(ret)
        return p
        

if __name__ == '__main__':
    ssh_controller_k8s = SshControlerK8s()

    ssh_controller_k8s.get_external_ip_for_inner_ip()

    # name = 'ticketinfo-service'
    # name = 'station-service'
    # container_id, local_port = ssh_controller_k8s.get_container_id_for_name(name)
    # print (container_id, local_port)
    # print (ssh_controller_k8s.node_name)

    # fault_id = ssh_controller_k8s.exec_network_delay(container_id, local_port, 200)
    # ssh_controller_k8s.exec_destroy_fault(fault_id)