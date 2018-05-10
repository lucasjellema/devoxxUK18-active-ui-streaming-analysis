// read events from one KAFKA TOPIC
// and publish to another

var KAFKA_ZK_SERVER_PORT = 2181;

var SOURCE_KAFKA_HOST = '129.150.77.116';
var SOURCE_TOPIC_NAME = 'a516817-tweetstopic';

var TARGET_KAFKA_HOST = '192.168.188.102';
var TARGET_TOPIC_NAME = 'tweets-topic';

var consumerOptions = {
    host: SOURCE_KAFKA_HOST + ':' + KAFKA_ZK_SERVER_PORT ,
    groupId: 'consume-tweets',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'latest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
  };
  
const kafka = require('kafka-node');
var topics = [SOURCE_TOPIC_NAME];
var consumerGroup = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

function onError (error) {
    console.error(error);
    console.error(error.stack);
  }
  
  function onMessage (message) {
    console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
    console.log("Tweet:"+message.value);
    var value = JSON.parse(message.value);
    if (value.tagFilter.startsWith('#')) {
      value.tagFilter=value.tagFilter.substr(1);
    }
    eventBridge.publishEvent(message.key,value)
  }
  
  process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
      consumer.close(true, callback);
    });
  });


  var eventBridge = module.exports;
  var Producer = kafka.Producer;
  var targetClient = new kafka.Client(TARGET_KAFKA_HOST);
  var producer = new Producer(targetClient);
  KeyedMessage = kafka.KeyedMessage;
  
  producer.on('ready', function () {
    console.log("Producer is ready in " + APP_NAME);
  });
  producer.on('error', function (err) {
    console.log("failed to create the client or the producer " + JSON.stringify(err));
  })
  
  
  let payloads = [
    { topic: TARGET_TOPIC_NAME, messages: '*', partition: 0 }
  ];
  

  eventBridge.publishEvent = function (eventKey, event) {
    km = new KeyedMessage(eventKey, JSON.stringify(event));
    payloads = [
      { topic: TARGET_TOPIC_NAME, messages: [km], partition: 0 }
    ];
    producer.send(payloads, function (err, data) {
      if (err) {
        console.error("Failed to publish event with key " + eventKey + " to topic " + TARGET_TOPIC_NAME + " :" + JSON.stringify(err));
      }
      console.log("Published event with key " + eventKey + " to topic " + TARGET_TOPIC_NAME + " :" + JSON.stringify(data));
    });
  }//publishEvent

