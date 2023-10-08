import threading

from locust.env import Environment
from .locust_sequential_tasks import UserGlobal
from locust.log import setup_logging
import logging


def start_locust_worker():
    setup_logging("INFO", None)
    logging.info('*******************')
    logging.info('{}'.format("Starting Worker"))
    logging.info('*******************')

    env = Environment(user_classes=[UserGlobal], host="http://192.168.2.12:32677")
    env.create_worker_runner(master_host="192.168.2.12", master_port=5557)
    env.runner.greenlet.join()
    print("Complete worker...")


if __name__ == '__main__':
    threads = []
    for i in range(2):
        t = threading.Thread(target=start_locust_worker)
        threads.append(t)

    for x in threads:
        x.start()

    for x in threads:
        x.join()
    print("Done")
