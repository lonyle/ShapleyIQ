There are two ways to prepare the dataset. The first way is to download the prepared dataset. The second is to run a TrainTicket microservice system and generate the dataset by injecting faults.

# 1. Download the prepared dataset
Download the datasets in [link](https://1drv.ms/u/s!AuhX-fJM-sJvhHJOWRy9IOK1sDbv?e=zqutJo), then unzip this **data.zip** as the folder **rca4tracing/fault_injection/data/**. After that, under the folder **rca4tracing/fault_injection/data/**, you will see a folder named **traces** which contains the anomalous/normal traces collected by Jaeger. Along with that, there are files with name similar to **ts-basic-service100_users5_spawn_rate5.json**, where "ts-basic-service100" means we inject a delay of 100ms to the service ts-basic-service, "users5" means there are 5 simulated users to visit the system and "spawn_rate5" means the spawn_rate is 5. 

# 2. Prepare the data using TrainTicket system from the scratch (not recommended, it could take more than one day)
## General Idea
We will first run the TrainTicket open-source system in Kubernetes. For the TrainTicket system, we only need two interfaces: (1) web host via http requests (2) observability data host via http requests. This hides the inner complexity of the TrainTicket system (and one could replace it with other systems).

Then, we use locust (https://locust.io/) to generate workloads to the (the locust workload generator is in the folder **rca4tracing/fault_injection/workload_generator**). This step is to simulate the workload under different settings. We only need the http web host for TrainTicket. 

During the simulation of users' workload, we use Chaosblade (https://github.com/chaosblade-io/chaosblade) to inject delays or other faults (e.g. 100% cpu/memory usage).

We collect the trace/metrics data from Jaeger/Prometheus which track the TrainTicket system.

## 2.1 How to prepare the environment to generate the data
1. visit the github repo: https://github.com/FudanSELab/train-ticket
2. Read the https://github.com/FudanSELab/train-ticket#quick-start and deploy it, we recommend you to deploy the Kubernetes version (with the pre-built images by the maintainers of TrainTicket).
3. Run the system with tracing (Jaeger) and with monitoring
```bash
make deploy DeployArgs="--with-tracing --with-monitoring"
```

After that, the Train Ticket web page will be at http://[Node-IP]:32677. The Jaeger host will be in http://[Node-IP]:32688

If you are unable to run TrainTicket using the repo https://github.com/FudanSELab/train-ticket because they have some upgradation, you can try the forked version: https://github.com/lonyle/train-ticket.

## 2.2 Install dependent softwares
1. Chaosblade (https://github.com/chaosblade-io/chaosblade). You may directly install the pre-built releases.
2. locust (https://locust.io/)
3. InfluxDB (https://www.influxdata.com/)
4. Redis (https://redis.com/)

## 2.3 Modify the config file 
The config file will be in **rca4tracing/fault_injection/config.py**
You should change the system_type to what you use (either "k8s" or "docker"). Note that if you use the prepared dataset, you should keep the system_type to "docker".
You should also adjust the web_host, jaeger_host, prom_host accordingly.

You should also pay attention to other parameters in the config file regarding to the IPs. 


## 2.4 (Optinonal) Forward a port from remote to local
(NOTE: This section only applies when you run the fault injection on other machines instead of where you deploy your TrainTicket system. You can skip this step if you are running the injection on the control plane node of the cluster.)


To enable remote ssh access without password, you should run the following:
```bash
ssh-copy-id username@remote_server_ip
```
Then, you need to forward the web_host port (e.g. 32677 for k8s), the jaeger_host port (e.g. 32688 for k8s) and the prom_host (e.g. 9090 for k8s) of the TrainTicket system to the localhost.
```bash
ssh -L <local_port>:localhost:<remote_port> username@localhost
```

## 2.5 Enable ssh connection from cluster leader to cluster follower
On your cluster leader node, excuete
```bash
ssh-copy-id [username]@[ip_of_follower_node]
```
For example, the user name of a follower node is `root` and the node ip is `10.10.1.2`, then you may run `ssh-copy-id root@10.10.1.2`.


## 2.6 After all these preparations
Just run the script **run_collect_data.sh** (which sets the variable collect_data to True)
```bash
bash run/run_collect_data.sh
```
This script will generate the input data, store the input data to **rca4tracing/fault_injection/data**, and use the generated data to run the experiments.
