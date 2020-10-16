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

class SeaZMQRouter{
    stop = false;
    commands = {}
    socket = undefined
    socket_poll_timeout = 1000
    constructor(definition){
        let ref = this;
        if(definition.bind != undefined){
            this.router_socket = new zmq.Router
            this.router_socket.receiveTimeout = this.socket_poll_timeout
            this.router_socket.bind(definition.bind).then(function(){
                ref.listener()
            })
        }else{
            throw new Error("No bind argument found for Router type device")
        }
        if(definition.publish != undefined){
            this.pubisher = SeaZMQPublisher(definition.publish)
        }
        this.commands = definition.commands
    }
    listener(){
        let ref = this;
        this.socket.receive().then(function(message){
            try{
                let route_id = message[0]
                let request = JSON.parse(message[1].toString("utf-8"))
                if(ref["get-last-value"] != undefined){
                    last_data = this.publisher.get_last_value(data["get-last-value"])
                    responder = SeaZMQResponder(data, this.router_socket, this.publisher, route_id)
                    responder.send({"last-value": last_data})
                }else if(ref.commands[request.command] != undefined){
                    let responder = new SeaZMQResponder(request,ref.socket,route_id)
                    ref.commands[request.command](responder)
                }
            }catch(error){
                console.log(error)
                console.log("JSON error occured")
            }
        }).catch(function(error){
            
        }).finally(function(){
            // after timeout, check if we should stop, if stop is needed, don't call loop to re-poll
            if(ref.stop != true){
                ref.listener()
            }
        })
    }
    stop(){
        this.stop = true;
    }
}

class SeaZMQResponder{
    request_data = undefined;
    socket = undefined;
    route_id = undefined
    constructor(request_data, socket, route_id, publisher, lvc=undefined){
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
    listener(){
        let ref = this;
        this.socket.receive().then(function(message){
            try{
                message = JSON.parse(message.toString("utf-8"))
                if(message["subscribe-to"] != undefined){
                    if(message["subscribe-topics"] != undefined){
                        response = message["response"]
                        topics = response["subscribe-topics"]
                        response_obj = ref.response_router[message["transaction-id"]]
                        for(topic of topics){
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
        }).catch(function(error){
            
        }).finally(function(){
            ref.set_timeouts()
            // after timeout, check if we should stop, if stop is needed, don't call loop to re-poll
            if(ref.stop != true){
                ref.listener()
            }
        })
    }
    add_stream(address, topic, subscriber, lvc){
        let ref = this
        ADD_SUBSCRIBER_LOCK.acquire("", function(){
            if(GLOBAL_SUBSCRIBERS[address] == undefined){
                GLOBAL_SUBSCRIBERS[address] = {}
            }
            if(GLOBAL_SUBSCRIBERS[address][topic] == undefined){
                let socket = new zmq.socket("sub")
                socket.connect(address)
                socket.receiveTimeout = 1000
                GLOBAL_SUBSCRIBERS[address][topic] = {
                    "subscribers": [subscriber],
                    "last-value": undefined,
                    "lock": new AsyncLock(),
                    "last-value-lock": new AsyncLock(),
                    "is-global": false,
                    "socket": socket,
                }
                ref.stream_listener(address,topic,socket)
            }else{
                GLOBAL_SUBSCRIBERS[address][topic]["lock"].acquire("", function(){
                    GLOBAL_SUBSCRIBERS[address][topic]["subscribers"].push(subscriber)
                })
            }
            ref.get_last_message(address, topic, subscriber, lvc)
        })
    }
    _update_subscribers(){
        if(GLOBAL_SUBSCRIBERS["address"] != undefined){
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
                                    if(Object.keys(GLOBAL_SUBSCRIBERS[topic]).length == 0){
                                        delete GLOBAL_SUBSCRIBERS[topic]
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
    async stream_listener(address,topic,socket){
        let ref = this
        socket.receive().then(function(message){
            console.log("pub message", message)
        }).catch(function(){
            console.log("timeout occured in sub listener")
        }).finally(function(){
            if(ref.stop != true){
                ref.stream_listener(address,topic,socket)
            }
        })
    }
    async _get_last_value(address, topic, subscriber, lvc){
        let ref = this
        if(GLOBAL_SUBSCRIBERS[address] != undefined){
            if(GLOBAL_SUBSCRIBERS[address][topic] != undefined){
                GLOBAL_SUBSCRIBERS[address][topic]["last-value-lock"].acquire("", function(){
                    if(GLOBAL_SUBSCRIBERS[address][topic]["last-value"] != undefined){
                        subscriber.update_stream(topic, GLOBAL_SUBSCRIBERS[address][topic]["last-value"])
                    }else{
                        sea_dealer = SeaZMQDealer({conn: lvc})
                        resp = sea_dealer.send({"get-last-value": topic})
                        resp.on("response", function(data){
                            if(data.response != undefined){
                                if(data.response["last-value"] != undefined){
                                    GLOBAL_SUBSCRIBERS[address][topic]["last-value"] = data["response"]["last-value"]
                                    subscriber.update_stream(topic, data["response"]["last-value"])
                                }
                            }
                        })
                        sea_dealer.stop_threads()
                    }
                })
            }
        }
    }
    set_timeouts(){
        let ref = this;
        this.timeout_array_lock.acquire("",function(){
            let i = 0;
            while(i<ref.timeout_array.length){
                if(ref.timeout_array[i][0] <= Date.now()){
                    ref.timeout_array[i][1].set_timed_out()
                    // remove transaction from available callbacks
                    delete ref.response_router[ref.timeout_array[i][1].get_sent_data()["transaction-id"]]
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
    stream_lock = new AsyncLock()
    stream = []
    last_topic_timestamp = {}
    closed = false
    response = undefined
    get_sent_data(){
        return this.sent_data
    }
    set_sent_data(data){
        this.sent_data = data
    }
    set_response(data){
        this.response = data
        this.emit("response", data)
    }
    set_timed_out(){
        if(!this.done){
            let json = {
                request: self.sent_data,
                response: "timeout"
            }
            this.emit("timeout", json)
            this.done = true
        }
    }
    set_streaming(){
        this.is_streaming = true
    }
    update_stream(topic, data){
        let ref = this
        if(this.streaming == false){
            this.set_streaming()
        }
        stream_lock.aquire("", function(){
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
}

module.exports = {SeaZMQResponder: SeaZMQResponder, SeaZMQResponse:SeaZMQResponse, SeaZMQDealer:SeaZMQDealer, SeaZMQRouter:SeaZMQRouter}