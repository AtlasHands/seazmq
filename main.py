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
                self.client_map[key] = SeaZMQServer(client_map[key])
            else:
                self.client_map[key] = SeaZMQClient(client_map[key])

    def stop(self):
        """
        Send signal to stop all active threads
        """
        for key in self.client_map:
            self.client_map[key].stop()


class SeaZMQServer:
    """
    SeaZMQRouter is an encapsulation of the ZMQ ROUTER socket type, it provides a lookup for callbacks as well as
    passing a SeaZMQResponder object to the callback
    """
    def __init__(self, definition):
        # set up stopping
        self.stop_threads = False
        self.publisher = None
        self.json = {}
        self.router_address = ""
        # if bind is present
        if "router" in definition:
            # setup zmq rep using the bind provided
            self.context = zmq.Context()
            self.router_address = definition["router"]
        else:
            print("No bind argument found for router type device")
            return
        if "publisher" in definition:
            self.publisher = SeaZMQPublisher(definition["publisher"])
        if "json" in definition:
            self.json = definition["json"]
            self.publisher.json = definition["json"]
        # set this rep sockets commands
        self.commands = definition["commands"]
        # start listening thread
        listener = threading.Thread(target=self.listener)
        listener.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def listener(self):
        """
        Listener that listens for incoming requests from a dealer
        """
        # listen until signaled to stop
        router = RouterSocket(self.router_address)
        while not self.stop_threads:
            try:
                router.recv_event.wait(1)
                # poll for data (safe for not thread blocking forever, and then never stopping)
                if router.recv_event.isSet():
                    handle_thread = threading.Thread(target=self._handle_request, args=[router])
                    handle_thread.start()

            except zmq.error.ZMQError:
                return
        router.stop()

    def _handle_request(self, router):
        # recv json
        data = {}
        try:
            recv = router.get_recv()
            route_id = recv[0]
            json_data = recv[1]
            # turn json into dict
            data = json.loads(json_data)
        except:
            print("json loading error")


        # get last data provides a topic to get data from
        if "get-last-value" in data:
            last_data = self.publisher.lvc.get_last_value(data["get-last-value"])
            responder = SeaZMQResponder(data, router, route_id, self.publisher, json=self.json)
            # if the last data is a list of elements, use "last-values" key to indicate it needs to be
            # unpacked
            if not isinstance(last_data, list):
                # if none gets turned into null
                responder.send({"last-value": last_data})
            else:
                responder.send({"last-values": last_data})
        # if callback exists, call it
        elif data["command"] in self.commands:
            # set up a responder so we can send it to whatever callback is assigned to this command
            responder = SeaZMQResponder(data, router, route_id, self.publisher,
                                        self.router_address, json=self.json)
            callback_thread = threading.Thread(target=self.commands[data["command"]], args=[responder])
            callback_thread.start()
        else:
            responder = SeaZMQResponder(data, router, route_id, self.publisher,
                                        self.router_address, json=self.json)
            responder.send("Server did not understand the request")

    def stop(self):
        """
            Send signal to stop all active threads
        """
        try:
            self.stop_threads = True
            self.publisher.lvc.stop()
            self.context.destroy(True)
        except:
            pass


class SeaZMQResponder:
    """
    SeaZMQResponder handles providing an easy object interface for communicating with a requester
    """
    def __init__(self, request_data, rep_socket, route_id, publisher, lvc=None, json=None):
        """
            Initialize data needed to send a packet back to the sender.

            :arg request_data: data sent by the requester
            :arg socket: socket related to the request
            :arg route_id: route_id given by router
        """
        self.request_data = request_data
        if json == None:
            self.json = {}
        else:
            self.json = json
        self.publish_lock = threading.Lock()
        self.rep_socket = rep_socket
        self.route_id = route_id
        self.publisher = publisher
        self.lvc = lvc

    def get_data(self):
        """ get requesters data (just in case sender has a flag they passed)"""
        return self.request_data

    def send_subscribe(self, address, topics, lvc=None):
        if lvc is not None:
            self.send({"subscribe-to": address, "subscribe-topics": topics, "lvc": lvc})
        elif self.lvc is not None:
            self.send({"subscribe-to": address, "subscribe-topics": topics, "lvc": self.lvc})
        else:
            self.send({"subscribe-to": address, "subscribe-topics": topics})

    def clear_sticky(self, topic, sticky_key):
        with self.publish_lock:
            data_dict = {}
            data_dict["timestamp"] = time.time()
            data_dict["clear-sticky"] = sticky_key
            self.publisher.send_string("%s %s" % (topic, json.dumps(data_dict, **self.json)))

    def publish(self, topic, data, sticky_key=None):
        with self.publish_lock:
            if self.publisher is not None:
                data_dict = {}
                data_dict["data"] = data
                if sticky_key is not None:
                    data_dict["set-sticky"] = sticky_key
                data_dict["timestamp"] = time.time()
                self.publisher.send_string("%s %s" % (topic, json.dumps(data_dict, **self.json)))
            else:
                print("Unable to publish event without a publisher defined")

    def send(self, data):
        """
        Send a response back to a requester.

        :param data: the data being sent (must be jsonable)
        """
        response_dict = {}
        transaction_id = self.request_data["transaction-id"]
        response_dict["transaction-id"] = transaction_id
        response_dict["response"] = data
        # router requires the route id to be 0 frame of multipart
        self.rep_socket.send_multipart(self.route_id, bytes(json.dumps(response_dict, **self.json), "utf-8"))


GLOBAL_SUBSCRIBERS = {}
ADD_SUBSCRIBER_LOCK = threading.Lock()

class SeaZMQClient:
    """
    SeaZMQReq is a wrapper for the ZMQ DEALER socket type, this class handles connecting to a DEALER socket, iterating
    an internal counter for response tracking and caller forwarding
    """
    def __init__(self, definition: dict, max_transaction_count=10000):
        # May have to have a start delay in the init
        self.counter = 0
        self.stop_event = threading.Event()
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
        if "conn" in definition:
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

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop()

    def stop(self):
        """
            Send signal to stop all active threads
        """
        try:
            self.stop_event.set()
            self.context.destroy(True)
        except:
            pass

    def response_listener(self):
        """
        Listen for responses from sent out transaction-id's
        """
        while not self.stop_event.is_set():
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
                                self.add_stream(message["response"]["subscribe-to"], i,
                                                self.response_router[message["transaction-id"]],
                                                message["response"]["lvc"])
                    else:
                        self.response_router[message["transaction-id"]].set_data(message)
                # check timeout array for any items that need to be timed out
                self.set_timeouts()
            # catch zmq errors by forcible close
            except:
                return

    def add_stream(self, address, topic, subscriber, lvc):
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
                    "sticky-values": {},
                    "lock": threading.Lock(),  # this lock is used when manipulating subscribers/sending to subscribers
                    "last-value-lock": threading.Lock(),
                    "is-global": False,  # global listeners don't need subscribers to remain live
                }
                start_stream_listener = threading.Thread(target=self.stream_listener, args=[address, topic])
                start_stream_listener.start()
            else:
                with GLOBAL_SUBSCRIBERS[address][topic]["lock"]:
                    GLOBAL_SUBSCRIBERS[address][topic]["subscribers"].append(subscriber)

        # every subscriber needs to get last message regardless of if a request is required
        get_last_message = threading.Thread(target=self._get_last_value, args=[address, topic, subscriber, lvc])
        get_last_message.start()

    def _update_subscribers(self, address, topic, data):
        if address in GLOBAL_SUBSCRIBERS:
            if topic in GLOBAL_SUBSCRIBERS[address]:
                with ADD_SUBSCRIBER_LOCK:
                    with GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"]:
                        if "set-sticky" in data:
                            GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"][data["set-sticky"]] = data
                        else:
                            GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data
                        index = 0
                        while index < len(GLOBAL_SUBSCRIBERS[address][topic]["subscribers"]):
                            # print("Updating subscribers")
                            if GLOBAL_SUBSCRIBERS[address][topic]["subscribers"][index].closed is True:
                                # print("Removing subscriber")
                                GLOBAL_SUBSCRIBERS[address][topic]["subscribers"].pop(index)
                                if len(GLOBAL_SUBSCRIBERS[address][topic]["subscribers"]) == 0:
                                    if GLOBAL_SUBSCRIBERS[address][topic]["is-global"] is False:
                                        # print("removed data obj")
                                        del GLOBAL_SUBSCRIBERS[address][topic]
                                        if len(GLOBAL_SUBSCRIBERS[address]) == 0:
                                            del GLOBAL_SUBSCRIBERS[address]
                                        # cannot iterate something that doesn't exist anymore
                                        return
                            else:
                                # print("Updating stream")
                                GLOBAL_SUBSCRIBERS[address][topic]["subscribers"][index].update_stream(topic, data)
                                index += 1

    # stream listener is always spawned after object initialization
    def stream_listener(self, address, topic):
        context = self.context
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(address)
        # subscribe to topic
        sub_socket.subscribe(topic)
        GLOBAL_SUBSCRIBERS[address][topic]["socket"] = sub_socket
        while not self.stop_event.is_set():
            try:
                has_data = sub_socket.poll(1000)  # prevent thread locking in case of requested stop
                # handle removing listeners with no object
                if address not in GLOBAL_SUBSCRIBERS:
                    return
                else:
                    if topic not in GLOBAL_SUBSCRIBERS[address]:
                        return
                if has_data != 0:
                    data = sub_socket.recv_string()
                    split = data.split(" ", 1)
                    json_data = json.loads(split[1])
                    # clear-sticky messages should not be added
                    if "clear-sticky" in data:
                        print("clear sticky heard")
                        with GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"]:
                            if data["clear-sticky"] in GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"]:
                                del GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"][data["clear-sticky"]]
                    else:
                        self._update_subscribers(address, topic, json_data)
            except:
                return

    # get last value for a single subscriber
    def _get_last_value(self, address, topic, subscriber, lvc):
        # check if data structure is already set up
        if address in GLOBAL_SUBSCRIBERS:
            if topic in GLOBAL_SUBSCRIBERS[address]:
                with GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"]:
                    last_values_local = False
                    # last value lock will release and give the next getter the last value if received
                    if GLOBAL_SUBSCRIBERS[address][topic]["last-value"] is not None:
                        subscriber.update_stream(topic, GLOBAL_SUBSCRIBERS[address][topic]["last-value"])
                        last_values_local = True
                    if len(GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"]):
                        for _, v in GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"].items():
                            subscriber.update_stream(topic, v, False)
                        last_values_local = True
                    if not last_values_local:
                        sea_dealer = SeaZMQClient({"conn": lvc})
                        # change to send to address and topic of sub
                        resp = sea_dealer.send({"get-last-value": topic})
                        while not self.stop_event.is_set():
                            try:
                                has_data = resp.response_event.wait(1)
                                if has_data != 0:
                                    data = resp.get_response()
                                    if "last-value" in data:
                                        if data["last-value"] is None:
                                            sea_dealer.stop()
                                            return
                                        if "set-sticky" in data["last-value"]:
                                            GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"][data["last-value"]["set-sticky"]] = data["last-value"]
                                        else:
                                            GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data["last-value"]
                                        subscriber.update_stream(topic, data["last-value"])
                                    if "last-values" in data:
                                        for last_value in data["last-values"]:
                                            if "set-sticky" in last_value:
                                                GLOBAL_SUBSCRIBERS[address][topic]["sticky-values"][
                                                        last_value["set-sticky"]] = last_value
                                            else:
                                                GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = last_value
                                            subscriber.update_stream(topic, last_value, False)
                                    sea_dealer.stop()
                                    return
                            except:
                                sea_dealer.stop()
                                return

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
        self.read_lock = threading.Lock()
        # done event that triggers when communication for a request is finished
        self.response_event = threading.Event()
        self.stream_lock = threading.Lock()
        self.stream_event = threading.Event()
        # last timestamp ensures that only new data is forwarded to callers
        self.last_topic_timestamp = {}
        self.stream = []
        # the holder for the data
        self.data = None
        self.closed = False
        self.timed_out = False
        self.is_streaming = False

    def close(self):
        self.closed = True

    def set_streaming(self):
        self.is_streaming = True

    def update_stream(self, topic, data, enforce_order=True):
        if not self.is_streaming:
            self.set_streaming()
        if self.closed:
            return
        with self.stream_lock:
            if "timestamp" in data:
                # old message
                if topic in self.last_topic_timestamp:
                    if enforce_order:
                        if self.last_topic_timestamp[topic] >= data["timestamp"]:
                            # print("Old Message")
                            return
                        else:
                            self.last_topic_timestamp[topic] = data["timestamp"]
                else:
                    self.last_topic_timestamp[topic] = data["timestamp"]
            data["topic"] = topic
            self.stream.append(data)
            # shift off first element to prevent using too much memory (typically won't happen)
            if len(self.stream) > 1000:
                self.stream = self.stream[1:]
            self.stream_event.set()

    def _get_timestamp(self, elem):
        if "timestamp" in elem:
            return elem["timestamp"]
        else:
            return 0

    def get_stream(self, reset_event=True):
        with self.stream_lock:
            self.stream.sort(key=self._get_timestamp)
            stream = copy.deepcopy(self.stream)
            self.stream = []
            if reset_event is True:
                self.stream_event.clear()
            return stream

    def set_timed_out(self):
        with self.read_lock:
            # request not finished before timeout invalidation, don't trigger timeout if streaming
            if not self.response_event.is_set() and not self.is_streaming:
                self.timed_out = True
                self.response_event.set()

    def set_data(self, data: dict):
        """
        set the data (not a stream)

        :param dict data: the data provided as a response to the request
        """
        with self.read_lock:
            # only respect the first traditional response
            if self.response_event.is_set() is False:
                if "transaction-id" in data:
                    del data["transaction-id"]
                self.data = data
                self.response_event.set()

    def get_request(self):
        return self.sent_data

    def get_response(self):
        """
        get the response data
        """
        with self.read_lock:
            if self.data is not None:
                if "response" in self.data:
                    if self.timed_out is True:
                        return {}, "timeout"
                    else:
                        return self.data["response"], None
                else:
                    return {}, "no response"
            if self.timed_out is True:
                return {}, "timeout"
            return {}, "no data"


# simple wrapper for publisher socket that creates an LVC
class SeaZMQPublisher:
    def __init__(self, bind):
        # cache_map
        self.send_lock = threading.Lock()
        self.cache_map = {}
        self.json = {}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(bind)
        self.socket.setsockopt(zmq.RECOVERY_IVL, 0)
        self.address = bind
        self.lvc = SeaZMQLVC(bind)

    def send_start_topic(self, topics):
        with self.send_lock:
            for i in range(topics):
                self.socket.send_string("%s %s", i, json.dumps({"topic-start: true"}, **self.json))

    def send_end_topic(self, topics):
        with self.send_lock:
            for i in range(topics):
                self.socket.send_string("%s %s", i, json.dumps({"topic-end: true"}, **self.json))

    def publish(self, topic, data, sticky_key=None):
        data_dict = {}
        data_dict["data"] = data
        if sticky_key is not None:
            data_dict["set-sticky"] = sticky_key
        data_dict["timestamp"] = time.time()
        self.send_string("%s %s" % (topic, json.dumps(data_dict, **self.json)))

    def send_string(self, message):
        with self.send_lock:
            self.socket.send_string(message)


class SeaZMQLVC:
    def __init__(self, sub_address=None):
        self.cache_map = {}
        self.sticky_cache_map = {}
        self.sticky_lock = threading.Lock()
        self.stop_event = False
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(sub_address)
        # subscribe to all topics
        self.socket.subscribe(b'')
        thread = threading.Thread(target=self.listen)
        thread.start()

    def get_last_value(self, topic):
        return_arr = []
        # get data with sticky_lock
        with self.sticky_lock:
            if topic in self.sticky_cache_map:
                for k, v in self.sticky_cache_map[topic].items():
                    return_arr.append(v)

        if topic in self.cache_map:
            return_arr.append(self.cache_map[topic])

        # return nothing if length of array is 0
        if len(return_arr) == 0:
            return None
        # return the element if length of array is 1 (array unpack handling)
        elif len(return_arr) == 1:
            return return_arr[0]
        # return the array (will set unpack=true in stream handling)
        else:
            return return_arr

    def listen(self):
        while not self.stop_event:
            try:
                has_results = self.socket.poll(1000)
                if has_results:
                    string_data = self.socket.recv_string()
                    split = string_data.split(" ", 1)
                    data = json.loads(split[1])
                    topic = split[0]
                    if "topic-end" in data:
                        self.cache_map[topic] = None
                    if "topic-start" in data:
                        self.cache_map[topic] = None
                    if "clear-sticky" in data:
                        # lock when mutating sticky
                        with self.sticky_lock:
                            # if the sticky key exists delete it
                            if topic in self.sticky_cache_map:
                                if data["clear-sticky"] in self.sticky_cache_map[topic]:
                                    del self.sticky_cache_map[topic][data["clear-sticky"]]
                                #  if our sticky topic is now empty delete it
                                if len(self.sticky_cache_map[topic]) == 0:
                                    del self.sticky_cache_map[topic]
                    else:
                        if "set-sticky" in data:
                            # lock when mutating sticky
                            with self.sticky_lock:
                                # only need to check if dict is in the topic
                                if topic not in self.sticky_cache_map:
                                    self.sticky_cache_map[topic] = {}
                                # assign the key to the data
                                self.sticky_cache_map[topic][data["set-sticky"]] = data
                        else:
                            # last non-sticky message
                            self.cache_map[topic] = data
            except:
                if self.stop_event is not True:
                    print("Unknown ZMQ error occurred")
                return

    def stop(self):
        self.stop_event = True
        self.context.destroy(True)


# router socket, created because of issues with uncatchable threading errors
class RouterSocket:
    def __init__(self, addr):
        self.addr = addr
        # internally holds data ready to send
        self.send_data = []
        # lock for mutating the send_data
        self.send_lock = threading.Lock()
        # holds data that has been received
        self.recv = []
        # locks the above recv data when mutating
        self.recv_lock = threading.Lock()
        # should the threads be stopped
        self.stop_event = False
        # recv event has been processed
        self.recv_cleared = threading.Event()
        # initially we have no recv, set recv_cleared
        self.recv_cleared.set()
        # lock for checking/mutating the state of events
        self.event_lock = threading.Lock()
        # is there an event currently? gets cleared when event_count == 0
        self.has_event = threading.Event()
        # is there a recv event
        self.has_recv = threading.Event()
        # is there a send event
        self.has_send = threading.Event()
        # external recv_event that implementors use
        self.recv_event = threading.Event()
        # thread that listens to the socket
        self.s_thread = threading.Thread(target=self._socket_listener)
        self.s_thread.start()

    def stop(self):
        self.stop_event = True

    # external callers can pop an element off of the current available received data
    def get_recv(self):
        data = None
        with self.recv_lock:
            if len(self.recv) != 0:
                data = self.recv[0]
            if len(self.recv) == 1:
                self.recv = []
                self.recv_event.clear()
            else:
                self.recv = self.recv[1:]
        return data

    # listen to the socket (and handle everything) in one thread
    def _socket_listener(self):
        # init router socket
        ctx = zmq.Context.instance()
        rep = ctx.socket(zmq.ROUTER)
        rep.bind(self.addr)
        # start the recv thread which will notify this thread if there are recv events
        recv_thread = threading.Thread(target=self._recv_listener, args=[rep])
        recv_thread.start()
        # while not told to stop
        while not self.stop_event:
            # wait on the has_event event
            self.has_event.wait(1)
            # if it is set
            if self.has_event.is_set():
                # check if the socket has available sends
                if self.has_send.is_set():
                    # with the sending lock
                    with self.send_lock:
                        # copy the send data, and set the queue to be empty
                        send_data = copy.copy(self.send_data)
                        self.send_data = []
                        # send all the current queue
                        for data in send_data:
                            rep.send_multipart(data)
                        # remove the corresponding number of events
                        with self.event_lock:
                            self.has_send.clear()

                if self.has_recv.is_set():
                    # 0.1ms poll check for data - trying to get all available data on rep and then relax to let sends
                    # get through
                    count = 0
                    has_data = rep.poll(0.1)
                    while has_data != 0:
                        with self.recv_lock:
                            self.recv.append(rep.recv_multipart())
                            self.recv_event.set()
                            count += 1
                        has_data = rep.poll(0.1)
                    with self.event_lock:
                        self.recv_cleared.set()
                        self.has_recv.clear()

                with self.event_lock:
                    if not self.has_send.is_set() and not self.has_recv.is_set():
                        self.has_event.clear()

    # just initing this thread with the socket just in case it is safer: http://api.zeromq.org/3-2:zmq#toc4
    def _recv_listener(self, socket):
        while not self.stop_event:
            # if has_recv is set (was already set by below code)
            if self.has_recv.is_set():
                # wait on the recv_cleared event
                self.recv_cleared.wait(1)
            # if the recv set is cleared (done on init, also done when recv finishes processing)
            if self.recv_cleared.is_set():
                # poll the socket
                has_data = socket.poll(1000)
                # if there is data
                if has_data != 0:
                    # clear the recv cleared event, set the recv event and the events event
                    with self.event_lock:
                        self.recv_cleared.clear()
                        self.has_recv.set()
                        self.has_event.set()

    def send_multipart(self, route, data):
        # with the send lock
        with self.send_lock:
            # append the data
            self.send_data.append([route, data])
        # with the event lock, set the send events
        with self.event_lock:
            self.has_send.set()
            self.has_event.set()

