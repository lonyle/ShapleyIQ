import json
import random
import string
import time

import requests
from numpy.random import randint

USER_CREDENTIALS = ["abcd", "efgh", "ijkl"]


class ApiTest:
    def __init__(self):
        self.username = "fdse_microservice"
        self.password = "111111"
        self.bearer = ""
        self.user_id = ""
        self.host = "http://192.168.2.11:32677"
        self.contactid = ""
        self.orderid = ""
        self.paid_orderid = ""

    def login(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json"}
        response = requests.post(url=self.host + "/api/v1/users/login",
                                 headers=head,
                                 json={
                                     "username": self.username,
                                     "password": self.password})
        print(response.text)
        response_as_json = response.json()["data"]
        if response_as_json is not None:
            token = response_as_json["token"]
            self.bearer = "Bearer " + token
            self.user_id = response_as_json["userId"]

    def search_ticket(self, departure_date, from_station, to_station, expected=True):
        head = {"Accept": "application/json",
                "Content-Type": "application/json"}
        body_start = {
            "startingPlace": from_station,
            "endPlace": to_station,
            "departureTime": departure_date
        }

        response = requests.post(
            url=self.host + "/api/v1/travelservice/trips/left",
            headers=head,
            json=body_start)
        if not response.json()["data"]:
            print("travel 2 service")
            response = requests.post(
                url=self.host + "/api/v1/travel2service/trips/left",
                headers=head,
                json=body_start)
        print(json.dumps(response.json()))
        for res in response.json()["data"]:
            self.trip_id = res["tripId"]["type"] + res["tripId"]["number"]
            self.start_station = res["startingStation"]
            self.terminal_station = res["terminalStation"]

    def get_foods(self):
        departure_date = "2021-04-23"
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        response = requests.get(
            url=self.host + "/api/v1/foodservice/foods/" + departure_date + "/shanghai/suzhou/D1345",
            headers=head)
        resp_data = response.json()
        if resp_data["data"]:
            if random.uniform(0, 1) <= 0.5:
                self.food_detail = {"foodType": 2,
                                    "foodName": resp_data["data"]["trainFoodList"][0]["foodList"][0]["foodName"],
                                    "foodPrice": resp_data["data"]["trainFoodList"][0]["foodList"][0]["price"]}
            else:
                self.food_detail = {"foodType": 1,
                                    "foodName":
                                        resp_data["data"]["foodStoreListMap"]["shanghai"][0]["foodList"][
                                            0]["foodName"],
                                    "foodPrice":
                                        resp_data["data"]["foodStoreListMap"]["shanghai"][0]["foodList"][
                                            0]["price"]}
        print(self.food_detail)
        # print(json.dumps(response.json()))

    def start_booking(self, from_station, expected=True):
        departure_date = "2021-04-21"
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        start_time = time.time()
        response = requests.get(
            url=self.host + "/client_ticket_book.html?tripId=" + self.trip_id + "&from=" + self.start_station + "&to=" + self.terminal_station + "&seatType=2&seat_price=50.0"
                                                                                                                                                 "&date=" + departure_date,
            headers=head)
        print(response)

    def select_contact(self, expected=True):
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        response_contacts = requests.get(
            url=self.host + "/api/v1/contactservice/contacts/account/" + self.user_id,
            headers=head)
        print(response_contacts.json())
        response_as_json_contacts = response_contacts.json()["data"]
        print(json.dumps(response_as_json_contacts))
        if len(response_as_json_contacts) == 0:
            response_contacts = requests.post(
                url=self.host + "/api/v1/contactservice/contacts",
                headers=head,
                json={
                    "name": self.user_id, "accountId": self.user_id, "documentType": "1",
                    "documentNumber": self.user_id, "phoneNumber": "123456"})

            response_as_json_contacts = response_contacts.json()["data"]
            # print(response_as_json_contacts)
            self.contactid = response_as_json_contacts["id"]
        else:
            self.contactid = response_as_json_contacts[0]["id"]

        print(self.contactid)

    def seat_service(self, departure):
        departure_date = departure
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        seat_select = {
            "date": departure_date,
            "from": self.start_station,
            "to": self.terminal_station
        }
        response = requests.post(
            url=self.host + "/api/v1/seatservice/seats",
            headers=head,
            json=seat_select)
        print(response.text)
        print(response.status_code)

    def finish_booking(self, dep):
        departure_date = dep
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        body_for_reservation = {
            "accountId": self.user_id,
            "contactsId": self.contactid,
            "tripId": self.trip_id,
            "seatType": "2",
            "date": departure_date,
            "from": self.start_station,
            "to": self.terminal_station,
            "assurance": "0",
            "foodType": 1,
            "foodName": "Bone Soup",
            "foodPrice": 2.5,
            "stationName": "",
            "storeName": ""
        }
        start_time = time.time()
        response = requests.post(
            url=self.host + "/api/v1/preserveservice/preserve",
            headers=head,
            json=body_for_reservation)
        print(response.text)
        print(response.status_code)

    def select_order(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        start_time = time.time()
        response_order_refresh = requests.post(
            url=self.host + "/api/v1/orderservice/order/refresh",
            headers=head,
            json={
                "loginId": self.user_id, "enableStateQuery": "false", "enableTravelDateQuery": "false",
                "enableBoughtDateQuery": "false", "travelDateStart": "null", "travelDateEnd": "null",
                "boughtDateStart": "null", "boughtDateEnd": "null"})

        response_as_json = response_order_refresh.json()["data"]
        print(response_as_json)
        # print(json.dumps(response_order_refresh.json()))
        for orders in response_as_json:
            if orders["status"] == 1:
                self.paid_orderid = orders["id"]
                break
        for orders in response_as_json:
            if orders["status"] == 0:
                self.orderid = orders["id"]

    def pay(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        start_time = time.time()
        response = requests.post(
            url=self.host + "/api/v1/inside_pay_service/inside_payment",
            headers=head,
            json={"orderId": self.orderid, "tripId": "D1345"})
        print(response.text)

    def collect_ticket(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        start_time = time.time()
        collect_ticket = requests.get(
            url=self.host + "/api/v1/executeservice/execute/collected/" + self.paid_orderid,
            headers=head)
        print(collect_ticket.text)

    def enter_station(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json", "Authorization": self.bearer}
        start_time = time.time()
        enter_station = requests.get(
            url=self.host + "/api/v1/executeservice/execute/execute/" + self.paid_orderid,
            headers=head)
        print(enter_station.text)


def random_string_generator():
    len = randint(8, 10)
    random_string = ''.join([random.choice(string.ascii_letters) for n in range(len)])
    return random_string


class AdminActions():
    def __init__(self):
        self.admin_user = "admin"
        self.admin_pwd = "222222"
        self.host = "http://192.168.2.11:32677"
        self.bearer = self.admin_login()
        self.user_credentials = []

    def admin_login(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json"}
        response = requests.post(url=self.host + "/api/v1/users/login",
                                 headers=head,
                                 json={
                                     "username": self.admin_user,
                                     "password": self.admin_pwd
                                 })
        return "Bearer " + response.json()["data"]["token"]

    def get_users(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": self.bearer}
        response = requests.get(url=self.host + "/api/v1/adminuserservice/users",
                                headers=head,
                                )
        print(response.text)

    def create_users(self):
        for username in USER_CREDETIALS:
            # username = random_string_generator()
            # print(username)
            payload = {"userName": username, "password": username, "gender": "1", "email": username + "@abc.com",
                       "documentType": "1", "documentNum": "1234567890"}
            head = {"Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": self.bearer}
            response = requests.post(url=self.host + "/api/v1/adminuserservice/users",
                                     headers=head,
                                     json=payload)
            if response.json()["status"] == 1:
                self.user_credentials.append(username)
                print(response.text)
        print(self.user_credentials)

    def get_all_orders(self):
        head = {"Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": self.bearer}
        response = requests.get(url=self.host + "/api/v1/adminorderservice/adminorder",
                                headers=head,
                                )
        return response.text

    def delete_orders(self, orderid, trainNumber):
        head = {"Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": self.bearer}
        response = requests.delete(
            url=self.host + "/api/v1/adminorderservice/adminorder/" + orderid + "/" + trainNumber,
            headers=head)
        print(response.text)


def basic_api_test():
    travel_date = random.randint(1, 30)
    dep = "2021-04-" + str(travel_date)
    stations = ["Shang Hai", "Tai Yuan", "Nan Jing", "Wu Xi", "Su Zhou"]
    from_station, to_station = random.sample(stations, 2)
    # print(from_station, to_station, dep)
    api_test = ApiTest()
    api_test.login()
    # api_test.select_contact()
    # api_test.search_ticket(dep, from_station, to_station)
    # api_test.get_foods()
    # api_test.finish_booking(dep)
    # api_test.search_ticket(dep, from_station, to_station)
    api_test.select_order()
    # api_test.enter_station()
    # api_test.seat_service()


def admin_api_test():
    admin_api = AdminActions()
    admin_api.create_users()
    # orders = admin_api.get_all_orders()
    # orders = json.loads(orders)
    # #print(orders[])
    # for order in orders["data"]:
    #     # print(order)
    #     #print(order["id"], order["trainNumber"])
    #     admin_api.delete_orders(order["id"], order["trainNumber"])
    #     # break


if __name__ == '__main__':
    # basic_api_test()
    admin_api_test()
