const seazmq = require("./seazmq.js")

function ping(ctx){
    ctx.send("ping")
    console.log("ping sent")
}
deal = new seazmq.SeaZMQDealer({conn: "tcp://127.0.0.1:90009"})
rout = new seazmq.SeaZMQRouter({bind: "tcp://127.0.0.1:90009", commands:{"ping":ping}})
setInterval(function(){
    let resp = deal.send({command: "ping"})
    resp.on("data", function(data){
        console.log("Heard Response: ")
        console.log(data)
    })
    resp.on("done", function(){
        console.log("End of stream")
    })
},500)