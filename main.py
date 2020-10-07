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
            self.context = zmq.Context()
            self.socket = self.context.socket(zmq.ROUTER)
            self.socket.bind(definition["bind"])
        else:
            print("No bind argument found for REP type device")
            return
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
                has_data = self.socket.poll(1000)
                # == 0 if no data is present
                if has_data != 0:
                    # recv route and json
                    route_id, json_data = self.socket.recv_multipart()
                    # turn json into dict
                    data = json.loads(json_data)
                    # if callback exists, call it
                    if self.commands[data["command"]] is not None:
                        # set up a responder so we can send it to whatever callback is assigned to this command
                        responder = SeaZMQResponder(data, self.socket, route_id)
                        # call callback, passing responder, leave it up to caller if they want to do sync,
                        # by default operates in sync
                        self.commands[data["command"]](responder)
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
    def __init__(self, request_data, socket, route_id):
        """
            Initialize data needed to send a packet back to the sender.

            :arg request_data: data sent by the requester
            :arg socket: socket related to the request
            :arg route_id: route_id given by router
        """
        self.request_data = request_data
        self.socket = socket
        self.route_id = route_id

    def get_data(self):
        """ get requesters data (just in case sender has a flag they passed)"""
        return self.request_data

    def send(self, data, is_stream=False, done=False):
        """
        Send a response back to a requester.

        :param data: the data being sent (must be jsonable)
        :param bool is_stream: indicate to the requester that the response has multiple packets
        :param bool is_stream: if the send is a stream, done indicates to the requester that its done with streaming
        """
        response_dict = {}
        transaction_id = self.request_data["transaction-id"]
        response_dict["stream"] = is_stream
        # only include done status if it's a stream, if it's not a single response indicates done
        if is_stream:
            response_dict["done"] = done
        response_dict["transaction-id"] = transaction_id

        response_dict["response"] = data
        # router requires the route id to be 0 frame of multipart
        self.socket.send_multipart([self.route_id, bytes(json.dumps(response_dict), "utf-8")])


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
                    # if its a stream, add to data array
                    if message["stream"] is True:
                        self.response_router[message["transaction-id"]].set_stream_data(message)
                    else:
                        # if its not a stream, set data
                        self.response_router[message["transaction-id"]].set_data(message)
                # check timeout array for any items that need to be timed out
                self.set_timeouts()
            # catch zmq errors by forcible close
            except zmq.error.ZMQError:
                pass

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
        # the holder for the data
        self.data = None

    def set_timed_out(self):
        with self.read_lock:
            # request not finished before timeout invalidation
            if not self.done_event.is_set():
                dict = {
                    "request": self.sent_data,
                    "response": "timeout",
                }
                self.data = dict
                self.done_event.set()

    def set_stream_data(self, data: dict):
        """
        sets the stream data, will append to an array of past data elements

        :param dict data: the data provided as a response to the request
        """
        with self.read_lock:
            # if we have not initialized the data list initialize it
            if self.data is None:
                self.data = []

            self.data.append(data)
            # received data, set the data event
            self.data_event.set()
            # if we indicate that it is done, set the done event
            if data["done"] is True:
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
