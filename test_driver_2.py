from main import *


def ping(ctx):
    ctx.send("ping")


device_map = {
    "ctrl-pi": {
        "conn": "tcp://127.0.0.1:80020",
    },
    "self": {
        "bind": "tcp://127.0.0.1:80025",
        "id": "rpi1",
        "commands": {
            "ping": ping
        }
    },
    "rpi2": {
        "conn": "tcp://127.0.0.1:80030"
    }
}

seazmq = SeaZMQ(device_map)
ctrl_pi = seazmq.client_map["ctrl-pi"]
rpi2 = seazmq.client_map["rpi2"]
