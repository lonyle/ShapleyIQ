import logging
import random

import gevent
from locust.env import Environment
from locust.log import setup_logging
from locust.stats import stats_printer, stats_history, StatsCSVFileWriter

from .locust_sequential_tasks import UserGlobal

global_user = 0


def spawn_users(env):
    global global_user
    while True:
        env.runner.start(env.runner.user_count + 1, spawn_rate=50)
        global_user += 1
        next_arrival = random.expovariate(1 / 50)
        gevent.sleep(next_arrival / 1000.0)


def start_locust_master():
    setup_logging("INFO", None)
    logging.info('*******************')
    logging.info('{}'.format("Starting Manager"))
    logging.info('*******************')

    env = Environment(user_classes=[UserGlobal], host="http://192.168.2.12:32677")
    env.create_local_runner()

    # env.create_master_runner(master_bind_host="192.168.2.12", master_bind_port=5557)

    env.create_web_ui("192.168.2.12", 8089)

    gevent.spawn(stats_printer(env.stats))
    gevent.spawn(stats_history, env.runner)
    csv_writer = StatsCSVFileWriter(
        environment=env,
        base_filepath="output/requests_p10_t2_2",
        full_history=True,
        percentiles_to_report=[0.5, 0.95, 0.99]
    )
    gevent.spawn(csv_writer)
    # pool = ThreadPool(2)
    # for x in range(2):
    #     gevent.spawn(spawn_users, env)
    gevent.spawn(spawn_users, env)
    # gevent.wait()
    gevent.spawn_later(30, lambda: env.runner.quit())

    env.runner.greenlet.join()
    # pool.join()

    env.web_ui.stop()
    print("Total User Created: ", global_user)
    print("complete test..")


if __name__ == '__main__':
    # master = threading.Thread(target=start_locust_master)
    # worker = threading.Thread(target=start_locust_worker)
    # master.start()
    # worker.start()
    # master.join()
    # worker.join()
    start_locust_master()
