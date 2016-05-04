from cassandra.cluster import Cluster

import time
import logging

log = logging.getLogger()
log.setLevel('INFO')
logging.basicConfig()

num_workers = 5
workers = []

cluster = Cluster(['127.0.0.1'])

for i in range(num_workers):
    workers.append(cluster.create_session_worker())

start = time.time()
for worker in workers:
    worker.start()

for worker in workers:
    worker.join()
stop = time.time()

print int(stop-start)
