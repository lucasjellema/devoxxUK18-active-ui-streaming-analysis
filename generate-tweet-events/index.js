// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application
var fs = require('fs')

var TOPIC_NAME = process.env.KAFKA_TOPIC || 'tweets-topic';
var kafkaHost = process.env.KAFKA_HOST || "192.168.188.102";
var zookeeperPort = process.env.ZOOKEEPER_PORT || 2181;

var kafka = require('kafka-node');
var Producer = kafka.Producer;
//        var client = new kafka.Client(EVENT_HUB_PUBLIC_IP + ':'+ZOOKEEPER_PORT);
var client = new kafka.Client(kafkaHost + ":" + zookeeperPort + "/")
var producer = new Producer(client);


let payloads = [
    { topic: TOPIC_NAME, messages: '*', partition: 0 }
];


producer.on('ready', function () {
    console.log("producer  is ready");
    produceTweetMessage();
    producer.send(payloads, function (err, data) {
        console.log("send is complete " + data);
        console.log("error " + err);
    });
});

var averageDelay = 10500;  // in miliseconds
var spreadInDelay = 500; // in miliseconds

var oowSessions = JSON.parse(fs.readFileSync('oow2017-sessions-catalog.json', 'utf8'));
var j1Sessions = JSON.parse(fs.readFileSync('javaone2017-sessions-catalog.json', 'utf8'));
var devoxxUKSessions = JSON.parse(fs.readFileSync('devoxxUK2018.json', 'utf8'));

console.log(oowSessions);

var prefixes = ["Interesting session ", "Come attend cool stuff ", "Enjoy insights ", "I hope to attend ", "See me present ", "Hey mum, I am a speaker "];

function guid() {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
    }
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
        s4() + '-' + s4() + s4() + s4();
}


function produceTweetMessage(param) {
    var prefix = prefixes[Math.floor(Math.random() * (prefixes.length))];
    var coin = Math.random();
    var tweetEvent = {
        "eventType": "tweetEvent"
        , "isARetweet": 'N'
        , "author": 'ActiveUI Tweet Generator'
        , "createdAt": null
        , "language": "en"
        , "tweetId": guid()
        , "originalTweetId": null
    };
    if (coin < 0.6) // devoxxUK
    {
        var sessionSeq = Math.floor((Math.random() * devoxxUKSessions.length));
        var session = devoxxUKSessions[sessionSeq];

        tweetEvent.tagFilter = "devoxxUK"
        tweetEvent.text = `${prefix} at DevoxxUK 2018 ${session.title} by ${session.speakers}`
        tweetEvent.hashTag ='Dev@London'
    }
    else // oracle code or java
    {
        var oow = coin < 0.75;
        var sessions = oow ? oowSessions : j1Sessions;
        var sessionSeq = sessions ? Math.floor((Math.random() * sessions.length)) : 1;
        var session = sessions[sessionSeq];
        if (!session.participants || !session.participants[0]) return;
        tweetEvent.tagFilter = oow ? 'oraclecode' : 'java';
        tweetEvent.text = `${prefix} at #${oow ? 'oraclecode' : 'java'} ${session.title}`
        tweetEvent.author = `${session.participants[0].firstName} ${session.participants[0].lastName}`
        tweetEvent.hashTag = sessions[sessionSeq].code
    }
    KeyedMessage = kafka.KeyedMessage,
        tweetKM = new KeyedMessage(tweetEvent.id, JSON.stringify(tweetEvent)),
        payloads[0].messages = tweetKM;

    producer.send(payloads, function (err, data) {
        if (err) {
            console.error(err);
        }
        console.log(data);
    });

    var delay = averageDelay + (Math.random() - 0.5) * spreadInDelay;
    //note: use bind to pass in the value for the input parameter currentCountry     
    setTimeout(produceTweetMessage.bind(null, 'somevalue'), delay);

}

producer.on('error', function (err) {
    console.error("Error " + err);
})