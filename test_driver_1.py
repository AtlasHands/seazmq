from main import *
import time

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
print("Done")
# client_2 [{'data': {'stage-1': 'test-finish-success', 'stage-2': 'test-finish-failure'}, 'timestamp': 1602715623.5249596, 'topic': 'test-status'}]
client_1.close()
client_2.close()
dealer.stop_threads()
router.stop_threads()
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