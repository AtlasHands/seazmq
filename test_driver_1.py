from main import *
import time


def ping(ctx: SeaZMQResponder):
    ctx.send_subscribe(ctx.publisher.address, ["status", "something-else"])
    count = 0
    ctx.publish("status", {"message": "new-status-" + str(count)})
    ctx.publish("something-else", {"message": "new-something-else-" + str(count)})
    time.sleep(.5)
    ctx.publish("status", {"message": "new-status-" + str(3)})
    ctx.publish("something-else", {"message": "new-status-" + str(3)})

device_map = {
    "self": {
        "bind": "tcp://127.0.0.1:80020",
        "publish": "tcp://127.0.0.1:80021",
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

responder = SeaZMQRouter({
    "bind": "tcp://127.0.0.1:80020",
    "publish": "tcp://127.0.0.1:80021",
    "id": "ctrl-pi",
    "commands": {
        "ping": ping
    }
})
dealer = SeaZMQDealer({
    "conn": "tcp://127.0.0.1:80020",
})
resp = dealer.send({"command": "ping"})
resp.close()
resp2 = dealer.send({"command": "ping"})
while 1:
    resp.stream_event.wait(1)
    print(resp.get_stream())
    resp2.stream_event.wait(1)
    print(resp2.get_stream())
