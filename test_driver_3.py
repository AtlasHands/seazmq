from main import *


def ping(ctx):
    ctx.send("ping")


device_map = {
    "ctrl-pi": {
        "conn": "tcp://127.0.0.1:80020",
    },
    "rpi1": {
        "conn": "tcp://127.0.0.1:80025",
    },
    "self": {
        "id": "rpi2",
        "bind": "tcp://127.0.0.1:80030",
        "commands": {
            "ping": ping
        }
    }
}

seazmq = SeaZMQ(device_map)
ctrl_pi = seazmq.client_map["ctrl-pi"]
rpi1 = seazmq.client_map["rpi1"]