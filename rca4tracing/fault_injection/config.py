# system_type = 'k8s'
system_type = 'docker'
web_host = 'http://localhost:32677'
jaeger_host = 'http://localhost:32688'
prom_host = 'http://localhost:9090'

k8s_remote_prefix = "ssh root@118.31.76.75"
k8s_blade_bin = "/root/chaosblade/target/chaosblade-1.5.0/blade"
k8s_node2external_ip = {
    'node1': '172.20.82.226', 
    'node2': '172.20.82.227', 
    'master': '172.20.82.224'
}