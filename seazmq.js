const zmq = require("zeromq")
var AsyncLock = require('async-lock');
const EventEmitter = require('events');


class SeaZMQ{
    client_map = {}
    constructor(client_map){
        let keys = Object.keys(client_map)
        for(let key of keys){
            if(key == "self"){
                this.client_map[key] = new SeaZMQRouter(client_map[key])
            }else{
                this.client_map[key] = new SeaZMQDealer(client_map[key])
            }
        }
    }
    stop(){
        let keys = Object.keys(this.client_map)
        for(let key of keys){
            this.client_map[key].stop()
        }
    }
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}   

async function _listener(socket){
    return new Promise(function(resolve, reject){
        socket.receive().then(function(message){
            resolve(message)
        }).catch(function(message){
            reject(message)
        })
    })
}

class SeaZMQRouter{
    stop = false;
    commands = {}
    socket = undefined
    socket_poll_timeout = 1000
    router_address = undefined
    publisher = undefined
    constructor(definition){
        let ref = this;
        if(definition.bind != undefined){
            this.router_socket = new zmq.Router
            this.router_socket.receiveTimeout = this.socket_poll_timeout
            this.router_address = definition.bind
            this.router_socket.bind(definition.bind).then(function(){
                ref.listener()
            })
        }else{
            throw new Error("No bind argument found for Router type device")
        }
        if(definition.publish != undefined){
            console.log("making publisher")
            ref.publisher = new SeaZMQPublisher(definition.publish)
        }
        this.commands = definition.commands
    }
    async listener(){
        let ref = this;
        while(ref.stop != true){
            try{
                let message = await _listener(this.router_socket)
                let route_id = message[0]
                try{
                    let request = JSON.parse(message[1].toString("utf-8"))
                    if(request["get-last-value"] != undefined){
                        let last_data = ref.publisher.get_last_value(request["get-last-value"])
                        let responder = new SeaZMQResponder(request, ref.router_socket, ref.publisher, route_id)
                        responder.send({"last-value": last_data})
                    }else if(ref.commands[request.command] != undefined){
                        let responder = new SeaZMQResponder(request, ref.router_socket, ref.publisher, route_id,this.router_address)
                        ref.commands[request.command](responder)
                    }
                }catch(error){
                    console.log(error)
                    console.log("JSON error occured")
                    let responder = new SeaZMQResponder("", this.router_socket, ref.publisher, route_id)
                    responder.send({"error": "json error occurred"})
                }
            }catch(error){
                await sleep(500)
                if(error.errno == 11){
                    console.log("poll-timeout")
                }else{
                    console.log(error)
                }
            }
        }
    }
    stop_threads(){
        this.stop = true;
        if(this.publisher != undefined){
            this.publisher.lvc.stop_threads()
        }
    }
}

class SeaZMQResponder{
    request_data = undefined;
    socket = undefined;
    route_id = undefined
    constructor(request_data, socket, publisher, route_id, lvc=undefined){
        this.request_data = request_data
        this.socket = socket
        this.route_id = route_id
        this.publisher = publisher
        this.lvc = lvc
    }
    get_data(){
        return JSON.parse(JSON.stringify(this.request_data))
    }
    send_subscribe(address, topics, lvc=undefined){
        let send_dict = {
            "subscribe-to": address,
            "subscribe-topics": topics,
        }
        if(lvc != undefined){
            send_dict.lvc = lvc
        }else if(this.lvc != undefined){
            send_dict.lvc = this.lvc
        }
        this.send(send_dict)
    }
    publish(topic, data){
        if(this.publisher != undefined){
            let data_dict = {}
            data_dict.data = data
            // ms to s
            data_dict.timestamp = Date.now()/1000
            this.publisher.send_string(topic + " " + JSON.stringify(data_dict))
        }else{
            console.log("Unable to publish event without a publisher defined")
        }
    }
    send(data){
        let response_json = {
            "transaction-id": this.request_data["transaction-id"],
        }
        response_json["response"] = data
        this.socket.send([this.route_id, JSON.stringify(response_json)]).catch(function(err){
            console.log(err)
        })
    }
}


GLOBAL_SUBSCRIBERS = {}
ADD_SUBSCRIBER_LOCK = new AsyncLock()


class SeaZMQDealer{
    stop = false
    counter = 0
    timeout_array = []
    timeout_array_lock = new AsyncLock()
    counter_lock = new AsyncLock()
    max_transaction_count  = undefined
    response_router = {}
    socket_poll_timeout = 1000
    socket = undefined
    constructor(definition, max_transaction_count=100000){
        let ref = this;
        let promises = []
        if(definition.conn != undefined){
            ref.socket = new zmq.Dealer
            ref.socket.receiveTimeout = this.socket_poll_timeout
            if(Array.isArray(definition.conn)){
                for(let conn of definition.conn){
                    let promise = ref.socket.connect(conn)
                    promises.push(promise)
                }
            }else{
                let promise = ref.socket.connect(definition.conn)
                promises.push(promise)
            }
        }else{
            console.log("No conn argument found for DEALER type device")
        }
        Promise.all(promises).then(function(){
            ref.listener()
        })
    }
    async listener(){
        let ref = this;
        console.log("Started dealer listener")
        while(this.stop != true){
            try{
                let message = await _listener(ref.socket)
                try{
                    message = JSON.parse(message.toString("utf-8"))
                    if(message["response"]["subscribe-to"] != undefined){
                        if(message["response"]["subscribe-topics"] != undefined){
                            let response = message["response"]
                            let topics = response["subscribe-topics"]
                            let response_obj = ref.response_router[message["transaction-id"]]
                            for(let topic of topics){
                                console.log("adding streams")
                                console.log(topic)
                                ref.add_stream(response["subscribe-to"], topic, response_obj, response["lvc"])
                            }
                        }
                    }else{
                        ref.response_router[message["transaction-id"]].set_response(message)
                    }
                }catch(error){
                    console.log(error)
                    console.log("JSON error occured")
                }
            }catch(error){
                if(error.errno == 11){
                    console.log("poll-timeout")
                }else{
                    console.log(error.errno)
                }
            }
            ref.set_timeouts()
        }
    }
    async add_stream(address, topic, subscriber, lvc){
        let ref = this
        ADD_SUBSCRIBER_LOCK.acquire("", function(){
            if(GLOBAL_SUBSCRIBERS[address] == undefined){
                GLOBAL_SUBSCRIBERS[address] = {}
            }
            if(GLOBAL_SUBSCRIBERS[address][topic] == undefined){
                GLOBAL_SUBSCRIBERS[address][topic] = {
                    "subscribers": [subscriber],
                    "last-value": undefined,
                    "lock": new AsyncLock(),
                    "last-value-lock": new AsyncLock(),
                    "is-global": false,
                }
                ref.stream_listener(address,topic)
            }else{
                GLOBAL_SUBSCRIBERS[address][topic]["lock"].acquire("", function(){
                    GLOBAL_SUBSCRIBERS[address][topic]["subscribers"].push(subscriber)
                })
            }
            sleep(10)
            ref._get_last_value(address, topic, subscriber, lvc)
        })
    }
    _update_subscribers(address, topic, data){
        if(GLOBAL_SUBSCRIBERS[address] != undefined){
            if(GLOBAL_SUBSCRIBERS[address][topic] != undefined){
                ADD_SUBSCRIBER_LOCK.acquire("", function(){
                    GLOBAL_SUBSCRIBERS[address][topic]["lock"].acquire("", function(){
                        GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data
                        let index = 0
                        while(index < GLOBAL_SUBSCRIBERS[address][topic].subscribers.length){
                            if(GLOBAL_SUBSCRIBERS[address][topic].subscribers[index].closed == true){
                                GLOBAL_SUBSCRIBERS[address][topic].subscribers.splice(index, 1)
                                if(GLOBAL_SUBSCRIBERS[address][topic].subscribers.length == 0){
                                    delete GLOBAL_SUBSCRIBERS[address][topic]
                                    if(Object.keys(GLOBAL_SUBSCRIBERS[address]).length == 0){
                                        delete GLOBAL_SUBSCRIBERS[address]
                                    }
                                    return
                                }
                            }else{
                                GLOBAL_SUBSCRIBERS[address][topic].subscribers[index].update_stream(topic, data)
                                index +=1
                            }
                        }
                    })
                })
            } 
        }
    }
    async stream_listener(address,topic){
        let ref = this
        let socket = new zmq.Subscriber
        socket.connect(address)
        socket.subscribe(topic)
        socket.receiveTimeout = 1000
        GLOBAL_SUBSCRIBERS[address][topic]["socket"] = socket
        while(ref.stop != true){
            try{
                let message = await _listener(socket)
                message = message.toString("utf-8")
                if(GLOBAL_SUBSCRIBERS[address] == undefined){
                    return
                }
                if(GLOBAL_SUBSCRIBERS[address][topic] == undefined){
                    return
                }
                let split = message.split(" ", 2)
                let data = JSON.parse(split[1].toString("utf-8"))
                this._update_subscribers(address, topic, data)
            }catch(error){
                if(error.errno == 11){
                    console.log("poll-timeout")
                }
                if(GLOBAL_SUBSCRIBERS[address] == undefined){
                    return
                }
                if(GLOBAL_SUBSCRIBERS[address][topic] == undefined){
                    return
                }
            }
        }
    }
    async _get_last_value(address, topic, subscriber, lvc){
        let ref = this
        if(GLOBAL_SUBSCRIBERS[address] != undefined){
            if(GLOBAL_SUBSCRIBERS[address][topic] != undefined){
                GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"].acquire("", function(){
                    if(GLOBAL_SUBSCRIBERS[address][topic]["last-value"] != undefined){
                        subscriber.update_stream(topic, GLOBAL_SUBSCRIBERS[address][topic]["last-value"])
                    }else{
                        let sea_dealer = new SeaZMQDealer({conn: lvc})
                        let resp = sea_dealer.send({"get-last-value": topic})
                        resp.on("response", function(data){
                            console.log("received data")
                            console.log(data)
                            if(data.response != undefined){
                                console.log()
                                if(data.response["last-value"] != undefined){
                                    console.log("last value receieved")
                                    GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data["response"]["last-value"]
                                    subscriber.update_stream(topic, data["response"]["last-value"])
                                }else{
                                    console.log("last value not defined")
                                }
                            }
                            sea_dealer.stop_threads()
                        })
                    }
                })
            }
        }
    }
    set_timeouts(){
        let ref = this;
        ref.timeout_array_lock.acquire("",function(){
            let i = 0;
            while(i<ref.timeout_array.length){
                if(ref.timeout_array[i][0] <= Date.now()){
                    ref.timeout_array[i][1].set_timed_out()
                    // remove transaction from available callbacks
                    delete ref.response_router[ref.timeout_array[i][1].get_request()["transaction-id"]]
                }else{
                    break
                }
                i+=1
            }
            ref.timeout_array = ref.timeout_array.slice(i)
        })
    }
    _insert_new_timeout(timeout, zmq_response){
        let ref = this;
        this.timeout_array_lock.acquire("",function(){
            let start = 0
            let end = ref.timeout_array.length
            if(end == 0){
                ref.timeout_array.push([timeout, zmq_response])
            }else{
                while(true){
                    let distance = Math.floor((end-start)/2)
                    let index = Math.floor(end-distance)
                    if(distance < 1){
                        ref.timeout_array.splice(index,0,[timeout,zmq_response])
                        return
                    }
                    let index_time = ref.timeout_array[index][0]
                    if(timeout < index_time){
                        end = index
                    }else if(timeout > index_time){
                        start = index
                    }else{
                        ref.timeout_array.splice(index, 0, [timeout,zmq_response])
                    }
                }
            }
        })
    }
    send(send_json, timeout=3){
        let ref = this;
        let sea_zmq_response = new SeaZMQResponse()
        ref.counter_lock.acquire("", function(){
            send_json["transaction-id"] = ref.counter
            let json_data = JSON.stringify(send_json)
            sea_zmq_response.set_sent_data(send_json)
            ref.response_router[ref.counter] = sea_zmq_response
            ref._insert_new_timeout(Date.now() + (timeout * 1000), sea_zmq_response)
            ref.counter += 1
            if(ref.counter > ref.max_transaction_count){
                ref.counter = 0
            }
            ref.socket.send(json_data)
        })
        return sea_zmq_response 
    }
    stop_threads(){
        this.stop = true
    }
}

class SeaZMQResponse extends EventEmitter{
    sent_data = {}
    data = undefined
    read_lock = new AsyncLock()
    stream_lock = new AsyncLock()
    stream = []
    last_topic_timestamp = {}
    closed = false
    is_streaming = false
    response = undefined
    response_received = false
    close(){
        this.closed = true
    }
    set_streaming(){
        this.is_streaming = true
    }
    set_sent_data(data){
        this.sent_data = data
    }
    set_timed_out(){
        if(this.response_received == false){
            let json = {
                request: this.sent_data,
                response: "timeout"
            }
            this.emit("timeout", json)
        }
    }
    set_response(data){
        let ref = this;
        ref.read_lock.acquire("", function(){
            if(ref.response_received != true){
                if(data["transaction-id"] != undefined){
                    delete data["transaction-id"]
                }
                ref.response_received = true
                ref.emit("response", data)
            }
        })
    }
    update_stream(topic, data){
        let ref = this
        if(this.streaming == false){
            this.set_streaming()
        }
        this.stream_lock.acquire("", function(){
            if(data.timestamp != undefined){
                if(ref.last_topic_timestamp[topic] >= data.timestamp){
                    return
                }else{
                    ref.last_topic_timestamp[topic] = data.timestamp
                }
            }
            data.topic = topic
            ref.emit("stream", data)
        })
    }
    get_request(){
        return this.sent_data
    }
}

class SeaZMQPublisher{
    send_lock = new AsyncLock()
    cache_map = {}
    socket = undefined
    address = undefined
    lvc = undefined
    constructor(bind){
        let ref = this
        ref.socket = new zmq.Publisher
        if(bind != undefined){
            ref.socket_promise = ref.socket.bind(bind).then(function(){

            }).catch(function(error){
                console.log("error occured when binding to zmq sock")
                console.log(error)
            })
            ref.lvc = new SeaZMQLVC(bind)
        }else{
            console.log("bind argument required for SeaZMQPublisher")
        }
    }
    get_last_value(topic){
        return this.lvc.cache_map[topic]
    }
    send_string(message){
        let ref = this
        this.send_lock.acquire("",function(){
            ref.socket_promise.then(function(){
                ref.socket.send(message)
                console.log("message published")
            })
        })
    }
}

class SeaZMQLVC{
    cache_map = {}
    stop = false
    socket = undefined
    constructor(sub_address){
        let ref = this
        this.socket = new zmq.Subscriber
        this.socket.connect(sub_address)
        ref.listen()
        ref.socket.subscribe()
        this.socket.receiveTimeout = 1000
    }
    async listen(){
        while(!this.stop){
            try{
                let message = await _listener(this.socket)
                try{
                    let string_data = message.toString("utf-8")
                    let split = string_data.split(" ", 2)
                    let topic = split[0].toString("utf-8")
                    let data = JSON.parse(split[1].toString("utf-8"))
                    this.cache_map[topic] = data
                    if(data["topic-end"] != undefined){
                        this.cache_map[topic] = undefined
                    }
                    if(data["topic-start"] != undefined){
                        this.cache_map[topic] = undefined
                    }
                }catch(error){
                    console.log(error)
                    console.log("error occurred when adding entry to lvc")
                }
            }catch(error){
                if(error.errno == 11){
                    console.log("poll-timeout")
                }else if(!this.stop){
                    console.log(error)
                }
            }
        }
    }
    stop_threads(){
        this.stop = true
    }
}



module.exports = {SeaZMQResponder: SeaZMQResponder, SeaZMQResponse:SeaZMQResponse, SeaZMQDealer:SeaZMQDealer, SeaZMQRouter:SeaZMQRouter}