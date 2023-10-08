from rca4tracing.fault_injection.generate_test import *

name = 'ticketinfo'
name = 'station'
container_id, local_port = get_container_id_for_name(name)
print (container_id, local_port)

# fault_id = exec_network_delay(container_id, 200)
# print (fault_id)

# # fault_id = 'ba4cf4451dc81260'
# exec_destroy_fault(fault_id)