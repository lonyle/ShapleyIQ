from locust import task, HttpUser, SequentialTaskSet
from locust.exception import StopUser
from requests.adapters import HTTPAdapter

from .locustfile import Requests


class SearchTicket(SequentialTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.mount('https://', HTTPAdapter(pool_maxsize=50))
        self.client.mount('http://', HTTPAdapter(pool_maxsize=50))

    @task
    def only_search(self):
        #logging.info("Running task 'only search'...")
        task_sequence = ["home_expected", "search_ticket_expected"]
        requests = Requests(self.client)
        for task in task_sequence:
            requests.perform_task(task)

    @task
    def stop(self):
        #logging.info("Stopping task 'only search'...")
        raise StopUser()


class BookTicket(SequentialTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.mount('https://', HTTPAdapter(pool_maxsize=50))
        self.client.mount('http://', HTTPAdapter(pool_maxsize=50))

    @task
    def book_ticket(self):
        #logging.info("Running Tasks for booking...")
        task_sequence = ["home_expected",
                         "login_expected",
                         "search_ticket_expected",
                         "start_booking_expected",
                         "get_assurance_types_expected",
                         "get_foods_expected",
                         "select_contact_expected",
                         "finish_booking_expected"]

        requests = Requests(self.client)
        for task in task_sequence:
            requests.perform_task(task)

    @task
    def stop(self):
        #logging.info("Stopping booking tasks")
        raise StopUser()


class ConsignTicket(SequentialTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.mount('https://', HTTPAdapter(pool_maxsize=50))
        self.client.mount('http://', HTTPAdapter(pool_maxsize=50))

    @task
    def perform_task(self):
       #logging.debug("Running tasks for 'consign ticket'...")
        task_sequence = [
            "home_expected",
            "login_expected",
            "select_contact_expected",
            "finish_booking_expected",
            "select_order_expected",
            "get_consigns_expected",
            "confirm_consign_expected",
        ]

        requests = Requests(self.client)
        for task in task_sequence:
            requests.perform_task(task)

    @task
    def stop(self):
        #logging.info("Stopping consign tasks")
        raise StopUser()


class PayForTickets(SequentialTaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.mount('https://', HTTPAdapter(pool_maxsize=50))
        self.client.mount('http://', HTTPAdapter(pool_maxsize=50))

    @task
    def perform_task(self):
        # logging.debug("Running tasks for 'pay'...")

        task_sequence = ["home_expected",
                         "login_expected",
                         "select_contact_expected",
                         "finish_booking_expected",
                         "select_order_expected",
                         "pay_expected"]

        requests = Requests(self.client)
        for task in task_sequence:
            requests.perform_task(task)

    @task
    def stop(self):
        #logging.info("Stopping pay tasks")
        raise StopUser()


class CollectTicketTasks(SequentialTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client.mount('https://', HTTPAdapter(pool_maxsize=50))
        self.client.mount('http://', HTTPAdapter(pool_maxsize=50))

    @task
    def perform_task(self):
        #logging.debug("Running user 'collect ticket'...")

        task_sequence = [
            "home_expected",
            "login_expected",
            "select_order_expected",
            "pay_expected",
            "collect_ticket_expected",
        ]

        requests = Requests(self.client)
        for task in task_sequence:
            requests.perform_task(task)

    @task
    def stop(self):
        #logging.info("Stopping collect ticket tasks")
        raise StopUser()


class UserGlobal(HttpUser):
    tasks = {
        SearchTicket: 1,
        BookTicket: 1,
        ConsignTicket: 1,
        PayForTickets: 1,
        CollectTicketTasks: 1
    }
