var kafka = require('kafka-node');
var tweetListener = module.exports;

var subscribers = [];
tweetListener.subscribeToTweets = function (callback) {
  subscribers.push(callback);
}

var kafkaHost = process.env.KAFKA_HOST || "192.168.188.102";
var zookeeperPort = process.env.ZOOKEEPER_PORT || 2181;
var TOPIC_NAME = process.env.KAFKA_TOPIC ||'tweets-topic';


//var client = new kafka.Client(kafkaHost + ":"+zookeeperPort+"/")

var consumerOptions = {
  host :kafkaHost+":"+zookeeperPort,
  groupId: 'consume-tweets-for-web-app',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var topics = [TOPIC_NAME];
var consumerGroup = new kafka.ConsumerGroup(Object.assign({ id: 'consumer1' }, consumerOptions), topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

function onMessage(message) {
  console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
  console.log("Message Value " + message.value)

  subscribers.forEach((subscriber) => {
    subscriber(message.value);

  })
}

function onError(error) {
  console.error(error);
  console.error(error.stack);
}

process.once('SIGINT', function () {
  async.each([consumerGroup], function (consumer, callback) {
    consumer.close(true, callback);
  });
});
