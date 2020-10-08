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
            this.socket = new zmq.Router
            this.socket.receiveTimeout = this.socket_poll_timeout
            this.socket.bind(definition.bind).then(function(){
                ref.listener()
            })
        }else{
            throw new Error("No bind argument found for Router type device")
        }
        this.commands = definition.commands
    }
    listener(){
        let ref = this;
        this.socket.receive().then(function(message){
            try{
                let route_id = message[0]
                let request = JSON.parse(message[1].toString("utf-8"))
                if(ref.commands[request.command] != undefined){
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
    constructor(request_data, socket, route_id){
        this.request_data = request_data
        this.socket = socket
        this.route_id = route_id
    }
    get_data(){
        return JSON.parse(JSON.stringify(this.request_data))
    }
    send(data, is_stream=false, done=false){
        let response_json = {
            "transaction-id": this.request_data["transaction-id"],
            "stream": is_stream
        }
        if(is_stream){
            response_json["done"]= done
        }
        response_json["response"] = data
        this.socket.send([this.route_id, JSON.stringify(response_json)]).catch(function(err){
            console.log(err)
        })
    }
}

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
                if(message.stream == true){
                    ref.response_router[message["transaction-id"]].set_stream_data(message)
                }else{
                    ref.response_router[message["transaction-id"]].set_data(message)
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
    done = false
    get_sent_data(){
        return this.sent_data
    }
    set_sent_data(data){
        this.sent_data = data
    }
    set_data(data){
        this.emit("data", data)
        this.emit("done", data)
        this.done = true
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
    set_stream_data(data){
        this.emit("data", data)
        if(data.done == true){
            this.emit("done")
            this.done = true
        }
    }
}

module.exports = {SeaZMQResponder: SeaZMQResponder, SeaZMQResponse:SeaZMQResponse, SeaZMQDealer:SeaZMQDealer, SeaZMQRouter:SeaZMQRouter}