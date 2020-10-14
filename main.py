import zmq
import json
import copy
import threading
import time
import math


class SeaZMQ:
    """
    SeaZMQ holds, and starts all of the classes relative to the client map passed, it also handles stopping them
    when necessary
    """
    def __init__(self, client_map):
        """
        Init a the map with valid connections and objects holding those connections

        :param client_map: map that follows the format:
        `
        {
            "self": { # self binds, is a dealer
                "bind": "tcp://<ip>:<port>",
                "id": "ctrl-pi", # could be used to sign requests
                "commands": {
                    <command-name>: <command-handler>
                }
            },
            "device": { # anything else connects, is a router
                "conn": "tcp://<ip>:<port>",
            },
        }
        `
        """
        self.client_map = {}
        for key in client_map:
            if key == "self":
                self.client_map[key] = SeaZMQRouter(client_map[key])
            else:
                self.client_map[key] = SeaZMQDealer(client_map[key])

    def stop(self):
        """
        Send signal to stop all active threads
        """
        for key in self.client_map:
            self.client_map[key].stop_threads()


class SeaZMQRouter:
    """
    SeaZMQRouter is an encapsulation of the ZMQ ROUTER socket type, it provides a lookup for callbacks as well as
    passing a SeaZMQResponder object to the callback
    """
    def __init__(self, definition):
        # set up stopping
        self.stop = False
        # if bind is present
        if definition["bind"] is not None:
            # setup zmq rep using the bind provided
            context = zmq.Context()
            self.router_socket = context.socket(zmq.ROUTER)
            self.router_socket.bind(definition["bind"])
        else:
            print("No bind argument found for REP type device")
            return
        if definition["publish"]:
            self.publisher = SeaZMQPublisher(definition["publish"])
        # set this rep sockets commands
        self.commands = definition["commands"]
        # start listening thread
        listener = threading.Thread(target=self.listener)
        listener.start()

    def listener(self):
        """
        Listener that listens for incoming requests from a dealer
        """
        # listen until signaled to stop
        while not self.stop:
            try:
                # poll for data (safe for not thread blocking forever, and then never stopping)
                has_data = self.router_socket.poll(1000)
                # == 0 if no data is present
                if has_data != 0:
                    # recv route and json
                    route_id, json_data = self.router_socket.recv_multipart()
                    # turn json into dict
                    data = json.loads(json_data)
                    # get last data provides a topic to get data from
                    if "get-last-value" in data:
                        last_data = self.publisher.lvc.get_last_value(data["get-last-value"])
                        responder = SeaZMQResponder(data, self.router_socket, self.publisher, route_id)
                        # if none gets turned into null
                        responder.send({"last-value": last_data})
                    # if callback exists, call it
                    elif self.commands[data["command"]] is not None:
                        # set up a responder so we can send it to whatever callback is assigned to this command
                        responder = SeaZMQResponder(data, self.router_socket, self.publisher, route_id)
                        callback_thread = threading.Thread(target=self.commands[data["command"]], args=[responder])
                        callback_thread.start()
            except zmq.error.ZMQError:
                pass

    def stop_threads(self):
        """
            Send signal to stop all active threads
        """
        self.stop = True
        self.context.destroy(True)


class SeaZMQResponder:
    """
    SeaZMQResponder handles providing an easy object interface for communicating with a requester
    """
    def __init__(self, request_data, router_socket, publisher, route_id):
        """
            Initialize data needed to send a packet back to the sender.

            :arg request_data: data sent by the requester
            :arg socket: socket related to the request
            :arg route_id: route_id given by router
        """
        self.request_data = request_data
        self.router_socket = router_socket
        self.publisher = publisher
        self.route_id = route_id

    def get_data(self):
        """ get requesters data (just in case sender has a flag they passed)"""
        return self.request_data

    def send_subscribe(self, address, topics):
        self.send({"subscribe-to": address, "subscribe-topics": topics})

    def publish(self, topic, data):
        data_dict = {}
        data_dict["data"] = data
        if "timestamp" not in data_dict:
            data_dict["timestamp"] = time.time()
        self.publisher.send_string("%s %s" % (topic, json.dumps(data_dict)))

    def send(self, data):
        """
        Send a response back to a requester.

        :param data: the data being sent (must be jsonable)
        :param bool is_stream: indicate to the requester that the response has multiple packets
        :param bool is_stream: if the send is a stream, done indicates to the requester that its done with streaming
        """
        response_dict = {}
        transaction_id = self.request_data["transaction-id"]
        response_dict["transaction-id"] = transaction_id
        response_dict["response"] = data
        # router requires the route id to be 0 frame of multipart
        self.router_socket.send_multipart([self.route_id, bytes(json.dumps(response_dict), "utf-8")])


GLOBAL_SUBSCRIBERS = {}
ADD_SUBSCRIBER_LOCK = threading.Lock()


class SeaZMQDealer:
    """
    SeaZMQReq is a wrapper for the ZMQ DEALER socket type, this class handles connecting to a DEALER socket, iterating
    an internal counter for response tracking and caller forwarding
    """
    def __init__(self, definition: dict, max_transaction_count=10000):
        # May have to have a start delay in the init
        self.counter = 0
        self.stop = False
        # sorted array of upcoming timeouts
        self.timeout_array = []
        # lock for adding timeouts/checking timeouts
        self.timeout_array_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        # the max transaction id number, lots of overhead by default
        self.max_transaction_count = max_transaction_count
        self.response_router = {}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        # the spec allows for multiple connections off a single dealer
        if definition["conn"] is not None:
            if isinstance(definition["conn"], list):
                for i in definition["conn"]:
                    self.socket.connect(i)
            else:
                self.socket.connect(definition["conn"])
        else:
            print("No conn argument found for REQ type device")
        # start listening to zmq socket in another thread
        listener = threading.Thread(target=self.response_listener)
        listener.start()

    def stop_threads(self):
        """
            Send signal to stop all active threads
        """
        self.stop = True
        self.context.destroy(True)

    def response_listener(self):
        """
        Listen for responses from sent out transaction-id's
        """
        while not self.stop:
            try:
                # check if we have a any data (thread safe)
                has_response = self.socket.poll(1000)
                if has_response != 0:
                    # router strips 0 frame of  multipart, so this is just json
                    message = self.socket.recv_json()
                    # received a command  to subscribe to a socket
                    if "subscribe-to" in message["response"]:
                        if "subscribe-topics" in message["response"]:
                            topics = message["response"]["subscribe-topics"]
                            for i in topics:
                                self.add_stream(message["response"]["subscribe-to"], i, self.response_router[message["transaction-id"]])
                    else:
                        self.response_router[message["transaction-id"]].set_data(message)
                # check timeout array for any items that need to be timed out
                self.set_timeouts()
            # catch zmq errors by forcible close
            except zmq.error.ZMQError:
                pass

    def add_stream(self, address, topic, subscriber):
        # need to have a lock over the top so that this process is not done twice for two spawning threads on the
        # same topic/address
        sub_initialization = False
        with ADD_SUBSCRIBER_LOCK:
            if address not in GLOBAL_SUBSCRIBERS:
                GLOBAL_SUBSCRIBERS[address] = {}
            # initialize
            if topic not in GLOBAL_SUBSCRIBERS[address]:
                GLOBAL_SUBSCRIBERS[address][topic] = {
                    "subscribers": [subscriber],
                    "last-value": None,
                    "lock": threading.Lock(),  # this lock is used when manipulating subscribers/sending to subscribers
                    "last-value-lock": threading.Lock()
                }
                start_stream_listener = threading.Thread(target=self.stream_listener, args=[address, topic])
                start_stream_listener.start()
                sub_initialization = True
            else:
                with GLOBAL_SUBSCRIBERS[address][topic]["lock"]:
                    GLOBAL_SUBSCRIBERS[address][topic]["subscribers"].append(subscriber)

        if sub_initialization:
            time.sleep(.005)
        # every subscriber needs to get last message regardless of if a request is required
        get_last_message = threading.Thread(target=self._get_last_value, args=[address, topic, subscriber])
        get_last_message.start()

    def _update_subscribers(self, address, topic, data):
        if address in GLOBAL_SUBSCRIBERS:
            if topic in GLOBAL_SUBSCRIBERS[address]:
                with GLOBAL_SUBSCRIBERS[address][topic]["lock"] and ADD_SUBSCRIBER_LOCK:
                    GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data
                    subscribers = GLOBAL_SUBSCRIBERS[address][topic]["subscribers"]
                    index = 0
                    while index < len(GLOBAL_SUBSCRIBERS[address][topic]["subscribers"]):
                        # print("Updating subscribers")
                        if GLOBAL_SUBSCRIBERS[address][topic]["subscribers"][index].closed is True:
                            print("Removing subscriber")
                            del GLOBAL_SUBSCRIBERS[address][topic]["subscribers"][index]
                        else:
                            GLOBAL_SUBSCRIBERS[address][topic]["subscribers"][index].update_stream(topic, data)
                            index += 1

    # stream listener is always spawned after object initialization
    def stream_listener(self, address, topic):
        context = zmq.Context()
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(address)
        # subscribe to topic
        sub_socket.subscribe(topic)
        GLOBAL_SUBSCRIBERS[address][topic]["socket"] = sub_socket
        while not self.stop:
            has_data = sub_socket.poll(1000)  # prevent thread locking in case of requested stop
            if has_data != 0:
                data = sub_socket.recv_string()
                split = data.split(" ", 1)
                json_data = json.loads(split[1])
                self._update_subscribers(address, topic, json_data)

    # get last value for a single subscriber
    def _get_last_value(self, address, topic, subscriber):
        # check if data structure is already set up
        if address in GLOBAL_SUBSCRIBERS:
            if topic in GLOBAL_SUBSCRIBERS[address]:
                with GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"]:
                    # last value lock will release and give the next getter the last value if received
                    if GLOBAL_SUBSCRIBERS[address][topic]["last-value"] is not None:
                        subscriber.update_stream(topic, GLOBAL_SUBSCRIBERS[address][topic]["last-value"])
                    else:
                        sea_dealer = SeaZMQDealer({"conn": "tcp://127.0.0.1:80020"})
                        # change to send to address and topic of sub
                        resp = sea_dealer.send({"get-last-value": topic})
                        while not self.stop:
                            has_data = resp.data_event.wait(1)
                            if has_data != 0:
                                data = resp.get_data()
                                if "response" in data:
                                    if "last-value" in data["response"]:
                                        if data["response"]["last-value"] is not None:
                                            subscriber.update_stream(topic, data["response"]["last-value"])
                                        break

    def set_timeouts(self):
        """
        check array of timeouts too see if any are invalidated
        """
        i = 0
        # with our timeout lock (adding in the middle would probably mess things up)
        with self.timeout_array_lock:
            # while we are still in the array
            while i < len(self.timeout_array):
                # if the current index is less than the current time (expired!)
                if self.timeout_array[i][0] <= time.time():
                    # set the object to be timed out, which in turn triggers the requester done_event
                    self.timeout_array[i][1].set_timed_out()
                else:
                    # since the array is ordered, as soon as we find a non-timed out request we know the
                    # rest are not timed out
                    break
                # go to the next element
                i += 1
            # shift off timed out elements
            self.timeout_array = self.timeout_array[i:]

    def _insert_new_timeout(self, timeout, zmq_response):
        """
        Simple binary search insertion that inserts a zmq response relative to its timeout into the timeout_array, since
        it implements the timeout array lock, callers don't have to

        :param timeout: the timeout associated with the request
        :param zmq_response: the response object that will be used to give the caller a response
        """
        # prevent multiple inserts at once (could be wrong)
        with self.timeout_array_lock:
            # set up initial start and end (full array)
            start = 0
            end = len(self.timeout_array)
            if end == 0:
                self.timeout_array.append([timeout, zmq_response])
            else:
                # while we have not found a position for insert
                while 1:
                    # distance / 2 minus end index give us the middle
                    distance = math.floor((end - start)/2)
                    index = math.floor(end - distance)
                    # if our distance is less than 1 the index is found for the insert
                    if distance < 1:
                        list.insert(self.timeout_array, index, [timeout, zmq_response])
                        # exit loop, found
                        return
                    # time associated with the index
                    index_time = self.timeout_array[index][0]
                    # if our timeout time is less than the current index
                    if timeout < index_time:
                        # shift end index to current index
                        end = index
                    # if our timeout time is greater than the current index
                    elif timeout > index_time:
                        # shift our start index to the current index
                        start = index
                    else:
                        # if the timeout is equal we can insert the element at the index of being equal
                        list.insert(self.timeout_array, index, [timeout, zmq_response])
                        # exit loop, found
                        return

    def send(self, send_map: dict, timeout=3):
        """
        Send a request to a host, setting a transaction id for tracking responses through the main listener

        :param send_map:
        :param int timeout: seconds for a timeout (send Math.inf) for no timeout - possibly for streams
        """
        # I know that you don't have to do this, but it signifies what's important coming out of the lock

        response_obj = None
        json_data = None
        with self.counter_lock:
            # set the transaction id of the json being sent
            send_map["transaction-id"] = self.counter
            json_data = json.dumps(send_map)
            # since this overwrites existing, no worries about subscribing old event to new caller
            sea_zmq_response = SeaZMQResponse(json_data)
            self.response_router[self.counter] = sea_zmq_response
            # insert a new timeout into the timeout array
            self._insert_new_timeout(time.time() + timeout, sea_zmq_response)
            response_obj = self.response_router[self.counter]
            self.counter += 1
            if self.counter > self.max_transaction_count:
                self.counter = 0

        self.socket.send_string(json_data)
        return response_obj


class SeaZMQResponse:
    """
    Response object that facilitates thread safe grabbing and entry of data in addition to clearing the event
    for future triggering (in the case of a stream)
    """
    def __init__(self, sent_data):
        """
        Initialize with a read lock, event to signal the caller data element and done indicator
        """
        self.sent_data = sent_data
        # read lock for data setting and getting
        self.read_lock = threading.Lock()
        # data event that triggers whenever a response comes through
        self.data_event = threading.Event()
        # done event that triggers when communication for a request is finished
        self.done_event = threading.Event()
        self.stream_lock = threading.Lock()
        self.stream_event = threading.Event()
        # last timestamp ensures that only new data is forwarded to callers
        self.last_topic_timestamp = {}
        self.stream = []
        # the holder for the data
        self.data = None
        self.closed = False
        self.is_streaming = False

    def close(self):
        self.closed = True

    def set_streaming(self):
        self.is_streaming = True

    def update_stream(self, topic, data):

        if not self.is_streaming:
            self.set_streaming()
        with self.stream_lock:
            if "timestamp" in data:
                # old message
                if topic in self.last_topic_timestamp:
                    if self.last_topic_timestamp[topic] >= data["timestamp"]:
                        return
                    else:
                        self.last_topic_timestamp[topic] = data["timestamp"]
                else:
                    self.last_topic_timestamp[topic] = data["timestamp"]
            data["topic"] = topic
            self.stream.append(data)
            self.stream_event.set()

    def get_stream(self, reset_event=True):
        with self.stream_lock:
            stream = copy.deepcopy(self.stream)
            self.stream = []
            if reset_event is True:
                self.stream_event.clear()
            return stream

    def set_timed_out(self):
        with self.read_lock:
            # request not finished before timeout invalidation
            if not self.done_event.is_set() and not self.is_streaming:
                dict = {
                    "request": self.sent_data,
                    "response": "timeout",
                }
                self.data = dict
                self.done_event.set()

    def set_data(self, data: dict):
        """
        set the data (not a stream)

        :param dict data: the data provided as a response to the request
        """
        with self.read_lock:
            # since its not a stream, both the data and done events get set when data is set
            self.data = data
            self.data_event.set()
            self.done_event.set()

    def get_data(self, pop=False):
        """
        get the response data

        :param bool pop: True if the grab should be destructive (in the case of streams)
        """
        with self.read_lock:
            if self.data is not None:
                self.data["request"] = self.sent_data
                data = copy.deepcopy(self.data)
                if pop:
                    self.data = None
                self.data_event.clear()
                return data


# simple wrapper for publisher socket that creates an LVC
class SeaZMQPublisher:
    def __init__(self, bind):
        # cache_map
        self.cache_map = {}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(bind)
        self.address = bind
        self.lvc = SeaZMQLVC(bind)

    def send_start_topic(self, topics):
        for i in range(topics):
            self.socket.send_string("%s %s", i, json.dumps({"topic-start: true"}))

    def send_end_topic(self, topics):
        for i in range(topics):
            self.socket.send_string("%s %s", i, json.dumps({"topic-end: true"}))

    def send_string(self, message):
        self.socket.send_string(message)


class SeaZMQLVC:
    def __init__(self, sub_address):
        self.cache_map = {}
        self.stop = False
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(sub_address)
        # subscribe to all topics
        self.socket.subscribe(b'')
        thread = threading.Thread(target=self.listen)
        thread.start()

    def get_last_value(self, topic):
        if topic in self.cache_map:
            return self.cache_map[topic]
        else:
            return None

    def listen(self):
        while not self.stop:
            has_results = self.socket.poll(1000)
            if has_results:
                string_data = self.socket.recv_string()
                split = string_data.split(" ", 1)
                data = json.loads(split[1])
                topic = split[0]
                self.cache_map[topic] = data

    def stop_threads(self):
        self.stop = True