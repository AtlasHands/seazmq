from main import *


def ping(ctx:SeaZMQResponder):
    ctx.send("fuck")


router = SeaZMQServer({
    "router": "tcp://127.0.0.1:8000",
    "publisher": "tcp://127.0.0.1:8001",
    "commands": {
        "ping": ping,
    }
})

thread_count = 100
sends_per_thread = 10

def send_thread(num):
    client = SeaZMQClient({"conn": "tcp://127.0.0.1:8000"})
    for i in range(sends_per_thread):
        t = threading.Thread(target=client_send_thread, args=[client, num, i])
        t.start()
    time.sleep(5)
    client.stop_threads()

def client_send_thread(sock, num, i):
    resp = sock.send({"command": "ping"})
    resp.response_event.wait()
    print(resp.get_response())

for i in range(thread_count):
    s = threading.Thread(target=send_thread, args=[i])
    s.start()

time.sleep(6)
router.stop_threads()






