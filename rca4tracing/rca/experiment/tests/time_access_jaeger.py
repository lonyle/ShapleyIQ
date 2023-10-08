import requests
import time

url = 'http://localhost:32688/api/traces/b37da96c2a597d2f'

start_time = time.time()
for i in range(100):
    response = requests.get(url)
print ('running time:', time.time()-start_time)

