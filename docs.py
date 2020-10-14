# ============== SERVERS ==============
class SeaZMQRouter:
    """
    Manages receiving commands from SeaZMQDealer.send(map), currently only objects that contain a "command" field are
    passed through to callbacks.
    """
    def __init__(self, definition):
        """
        arguments:
            definition: dict    {
                                    "bind": "tcp://<address>:port",
                                    "publish": "tcp://<address>:port"
                                    "commands": {
                                        "command-name": command_callback
                                    }
                                }
        notes:
            command_callbacks are called with a SeaZMQResponder object passed to them
        """


class SeaZMQResponder:
    """
    Facilitates an easy api for responding/publishing data.
    """
    def send(self, data):
        """
        arguments:
            data: jsonable  data gets passed through json.dumps and therefor must have the ability to be jsonable.
                            (basic data types left by themselves are jsonable)
                            Sends a response to a SeaZMQDealer (responding to the command through the callback).
                            Only one response is respected by SeaZMQResponse objects, so send should only be called once
                            (the idea is that if streams are needed you should subscribe the client to a publisher
                            topic)
        """

    def publish(self, topic, data):
        """
        arguments:
            topic: string   zmq publisher topic to publish into
            data: jsonable  data gets passed through json.dumps and therefor must have the ability to be jsonable.
                            (basic data types left by themselves are jsonable)
                            published data gets pushed out with a timestamp of when publish was called by the callback.
                            callback may provide a manual timestamp.  However, SeaZMQResponse only adds the latest
                            data to the current stream (if data is older it will not ) that is present within that
                            publisher topic.
        """


# ============== CLIENTS ==============
class SeaZMQDealer:
    """
    Sets up an async available sender that sends to a SeaZMQRouter
    Takes in a definition object that contains a "conn" field specifying
    the receiving SeaZMQRouter address and port. Ex: "tcp://<address>:<port>"

    SeaZMQDealer also interfaces to setup listeners to SeaZMQPublisher sockets
    updating and removing unused streams as needed and optimizing requests to
    the SeaZMQLVC present on SeaZMQPublishers by holding and sharing its own
    internal cache with new subscribers.
    """
    def __init__(self, definition, max_transaction_count=10000):
        """
        arguments:
            definition: dict                uses a dictionary of: {conn: "tcp://<address>:<port>"} or
                                            {conn: ["tcp://<address>:<port>",....]} to initialize a connection to a
                                            SeaZMQRouter
            max_transaction_count: integer  used as a circular counter to sign requests/response pairs to be unique.
                                            For traditional request/response communication the max count should be
                                            higher than the amount of requests/responses achievable within the minimum
                                            timeout.
        """

    def send(self, send_map, timeout=3):
        """
        arguments:
            send_map: dictionary    since SeaZMQ sends json, the send method only accepts dictionaries
            timeout: integer        time in seconds to wait for a traditional response, a timeout signals the done_event
                                    on the SeaZMQResponse object where the response field is a string with the value
                                    "timeout"
         returns:
            SeaZMQResponse
        """
        return SeaZMQResponse


class SeaZMQResponse:
    """
    Sets up an easy api for receiving data from traditional requests and streams.  This object is returned by
    SeaZMQRouter.send(dict) and shouldn't need to be created manually.

    class objects:
        response_event: threading.Event - sets when a response is heard that is traditional via
                        SeaZMQResponder.send(dictionary)
        stream_event:   threading.Event - triggers when a new publish comes through to a subscribed topic
                        via SeaZMQResponder.publish(topic, dictionary).
    """
    def get_stream(self):
        """
        Safely gets the latest representation of the streams assigned this response object
        by SeaZMQResponder.send_subscribe(address, topics).  This method clears the class stream_event thread event
        allowing stream_event to be encapsulated in a event.wait() loop.
        returns:
            Array containing the buffer of stream events (destructive, what is read is removed from buffer).
        """
    def get_response(self):
        """
        Get the data representation of a traditional response using SeaZMQResponder.send(dictionary).
        the response is encapsulated in the "response" key in the returned dictionary.  The original request is
        encapsulated in the "request" key in the returned dictionary. No timestamp is given, if time tracking needs to
        be done it should be done by passing the time in the response object.
        """
    def close(self):
        """
        Indicates to to objects feeding this class data that it no longer wants data, effectively unsubscribing it and
        freeing memory/cpu time.
        """


# ============== Examples ==============

# ===== Simple Request/Reply =====

# Ping endpoint
def ping(ctx):
    ctx.send("pong!")


router = SeaZMQRouter({
    "bind": "tcp://127.0.0.1:8000",
    "commands": {
        "ping": ping,
    }
})

dealer = SeaZMQDealer({"conn": "tcp://127.0.0.1:8000"})
response = dealer.send({"command": "ping"})

response.response_event.wait()
print(response.get_response())
# {'response': 'pong!', 'request': '{"command": "ping", "transaction-id": 0}'}

# ===== End of Simple Request/Reply =====

# ===== Singleton process w/multi subscribers =====
test_in_progress_event = threading.Event()
test_in_progress_lock = threading.Lock()

def start_test(ctx):
    ctx.send_subscribe(ctx.publisher.address, ["test-status", "device-info"])
    with test_in_progress_lock:
        if test_in_progress_event.is_set():
            ctx.send("Test Already in Progress")
            return
        else:
            test_in_progress_event.set()
            ctx.send("Test Started")

    ctx.publish("test-status", {
        "stage-1": "started",
        "stage-2": "not started"
    })
    ctx.publish("device-info", {
        "light": "yes",
        "air": "no"
    })
    time.sleep(1)
    ctx.publish("test-status", {
        "stage-1": "test-finish-success",
        "stage-2": "started",
    })
    time.sleep(1)
    ctx.publish("test-status", {
        "stage-1": "test-finish-success",
        "stage-2": "test-finish-failure",
    })
    with test_in_progress_lock:
        test_in_progress_event.clear()

router = SeaZMQRouter({
    "bind": "tcp://127.0.0.1:8000",
    "publish": "tcp://127.0.0.1:8001",
    "commands": {
        "start-test": start_test,
    }
})

dealer = SeaZMQDealer({"conn": "tcp://127.0.0.1:8000"})
client_1 = dealer.send({"command": "start-test"})

# unwrapped for easier print viewing.

client_1.response_event.wait()
print("client_1", client_1.get_response())
# client_1 {'response': 'Test Started', 'request': '{"command": "start-test", "transaction-id": 0}'}

client_1.stream_event.wait()
print("client_1", client_1.get_stream())
# client_1 [{'data': {'stage-1': 'started', 'stage-2': 'not started'}, 'timestamp': 1602715037.6239266, 'topic': 'test-status'}]
client_1.stream_event.wait()
print("client_1", client_1.get_stream())
# client_1 [{'data': {'light': 'yes', 'air': 'no'}, 'timestamp': 1602715383.419707, 'topic': 'device-info'}]


client_2 = dealer.send({"command": "start-test"})

client_2.response_event.wait()
print("client_2", client_2.get_response())
# client_2 {'response': 'Test Already in Progress', 'request': '{"command": "start-test", "transaction-id": 1}'}

client_2.stream_event.wait()
print("client_2", client_2.get_stream())
# client_2 [{'data': {'stage-1': 'started', 'stage-2': 'not started'}, 'timestamp': 1602715621.5179632, 'topic': 'test-status'}, {'data': {'light': 'yes', 'air': 'no'}, 'timestamp': 1602715621.5179632, 'topic': 'device-info'}]

client_1.stream_event.wait()
print("client_1", client_1.get_stream())
# client_1 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'started'}, 'timestamp': 1602715622.5243034, 'topic': 'test-status'}]

client_2.stream_event.wait()
print("client_2", client_2.get_stream())
# client_2 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'started'}, 'timestamp': 1602715622.5243034, 'topic': 'test-status'}]

client_1.stream_event.wait()
print("client_1", client_1.get_stream())
# client_1 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'test-finish-failure'}, 'timestamp': 1602715623.5249596, 'topic': 'test-status'}]

client_2.stream_event.wait()
print("client_2", client_2.get_stream())
# client_2 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'test-finish-failure'}, 'timestamp': 1602715623.5249596, 'topic': 'test-status'}]
client_1.close()
client_2.close()
# client_1 {'response': 'Test Started', 'request': '{"command": "start-test", "transaction-id": 0}'}
# client_1 [{'data': {'stage-1': 'started', 'stage-2': 'not started'}, 'timestamp': 1602715708.5011637, 'topic': 'test-status'}]
# client_1 [{'data': {'light': 'yes', 'air': 'no'}, 'timestamp': 1602715708.5011637, 'topic': 'device-info'}]
# client_2 {'response': 'Test Already in Progress', 'request': '{"command": "start-test", "transaction-id": 1}'}
# client_2 [{'data': {'stage-1': 'started', 'stage-2': 'not started'}, 'timestamp': 1602715708.5011637, 'topic': 'test-status'}, {'data': {'light': 'yes', 'air': 'no'}, 'timestamp': 1602715708.5011637, 'topic': 'device-info'}]
# client_1 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'started'}, 'timestamp': 1602715709.5021574, 'topic': 'test-status'}]
# client_2 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'started'}, 'timestamp': 1602715709.5021574, 'topic': 'test-status'}]
# client_1 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'test-finish-failure'}, 'timestamp': 1602715710.5024981, 'topic': 'test-status'}]
# client_2 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'test-finish-failure'}, 'timestamp': 1602715710.5024981, 'topic': 'test-status'}]

# ===== End of Singleton process w/multi subscribers =====
