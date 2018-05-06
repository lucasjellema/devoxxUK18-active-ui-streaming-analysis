var kafka = require('kafka-node');


var tweetLikeProducer = module.exports;

var TOPIC_NAME = process.env.KAFKA_TOPIC || 'tweet-like-topic';
var kafkaHost = process.env.KAFKA_HOST || "192.168.188.102";
var zookeeperPort = process.env.ZOOKEEPER_PORT || 2181;


var Producer = kafka.Producer;
//var client = new kafka.Client(EVENT_HUB_PUBLIC_IP + ':' + ZOOKEEPER_PORT);
var client = new kafka.Client(kafkaHost + ":" + zookeeperPort + "/")
var producer = new Producer(client);

let payloads = [
  { topic: TOPIC_NAME, messages: '*', partition: 0 }
];

tweetLikeProducer.produceTweetLike = function (tweetLikeEvent) {
  var tle = JSON.parse(JSON.stringify(tweetLikeEvent));
  tle.eventType = "tweetLikeEvent";
  KeyedMessage = kafka.KeyedMessage,
    tweetKM = new KeyedMessage(tweetLikeEvent.tweetId, JSON.stringify(tle)),
    payloads[0].messages = tweetKM;

  producer.send(payloads, function (err, data) {
    if (err) {
      console.error(err);
    }
    console.log("published tweetLikeEVent" + data);
  });
}//produceTweetLike
