from main import *
import time


def ping(ctx: SeaZMQResponder):
    ctx.send("ping")


device_map = {
    "self": {
        "bind": "tcp://127.0.0.1:80020",
        "id": "ctrl-pi",
        "commands": {
            "ping": ping
        }
    },
    "rpi1": {
        "conn": "tcp://127.0.0.1:80025",
    },
    "rpi2": {
        "conn": "tcp://127.0.0.1:80030"
    }
}
# initialize device map
seazmq = SeaZMQ(device_map)
# quick references for objects
rpi1 = seazmq.client_map["rpi1"]
rpi2 = seazmq.client_map["rpi2"]
# timeout is default 3 seconds
while 1:
    ping_resp = rpi2.send({"command": "ping"})
    ping_resp.done_event.wait(1000)
    if ping_resp.done_event.is_set():
        data = ping_resp.get_data()
        print(data)
    else:
        print("No response received!")
    time.sleep(3)
# stop threads
seazmq.stop()
