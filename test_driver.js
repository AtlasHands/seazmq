const seazmq = require("./seazmq.js")


function simpleRequestResponse(){
    function ping(ctx){
        ctx.send("ping")
        console.log("ping sent")
    }
    deal = new seazmq.SeaZMQDealer({conn: "tcp://127.0.0.1:90009"})
    rout = new seazmq.SeaZMQRouter({bind: "tcp://127.0.0.1:90009", commands:{"ping":ping}})
    setInterval(function(){
        let resp = deal.send({command: "ping"})
        resp.on("response", function(data){
            console.log("Heard Response: ")
            console.log(data)
        })
        resp.on("timeout", function(){
            console.log("caller-timeout")
        })
        resp.on("done", function(){
            console.log("End of stream")
        })
    },500)
}


async function singletonProcessWithMultiSubscribers(){
    test_in_progress = false
    async function start_test(ctx){
        ctx.send_subscribe("tcp://127.0.0.1:8001", ["test-status", "device-info"])
        if(test_in_progress){
            ctx.send("test already in progress")
            return
        }else{
            test_in_progress = true
            ctx.send("test started")
        }
        ctx.publish("test-status", {
            "stage-1": "started",
            "stage-2": "not started"
        })
        ctx.publish("device-info", {
            "light": "yes",
            "air": "no"
        })
        await sleep(1000)
        ctx.publish("test-status", {
            "stage-1": "test-finish-success",
            "stage-2": "started",
        })
        ctx.publish("device-info", {
            "light": "no",
            "air": "yes"
        })
        await sleep(1000)
        ctx.publish("test-status", {
            "stage-1": "test-finish-success",
            "stage-2": "test-finish-failure",
        })
        test_in_progress = false
    }
    let router = new seazmq.SeaZMQRouter({
        "bind": "tcp://127.0.0.1:8000",
        "publish": "tcp://127.0.0.1:8001",
        "commands": {
            "start-test": start_test,
        }
    })
    
    let dealer = new seazmq.SeaZMQDealer({"conn": "tcp://127.0.0.1:8000"})
    let client_1 = dealer.send({"command": "start-test"})

    client_1.on("response", function(message){
        console.log("client_1", message)
    })
    client_1.on("stream", function(message){
        console.log("client_1", message)
    })
    await sleep(1000)
    let client_2 = dealer.send({"command": "start-test"})
    client_2.on("response", function(message){
        console.log("client_2", message)
    })
    client_2.on("stream", function(message){
        console.log("client_2", message)
    })
    await sleep(5000)
    client_2.close()
    client_1.close()
    dealer.stop_threads()
    router.stop_threads()
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
singletonProcessWithMultiSubscribers()

// simpleRequestResponse()