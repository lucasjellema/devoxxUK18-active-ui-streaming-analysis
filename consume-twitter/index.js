var Twit = require('twit');
const express = require('express');
const app = express();

const { twitterconfig } = require('./twitterconfig');

const bodyParser = require('body-parser');
app.use(bodyParser.json());

var T = new Twit({
  consumer_key: twitterconfig.consumer_key,
  consumer_secret: twitterconfig.consumer_secret,
  access_token: twitterconfig.access_token_key,
  access_token_secret: twitterconfig.access_token_secret,
  timeout_ms: 60 * 1000,
});


//var hashtag = "devoxxUK";
var twiterHashTags = process.env.TWITTER_HASHTAGS||'oraclecode,java,devoxxUK';
var tracks = { track: twiterHashTags.split(',') };

let tweetStream = T.stream('statuses/filter', tracks)
tweetstream(tracks, tweetStream);

function tweetstream(hashtags, tweetStream) {
  //  tweetStream.stop();
  // tweetStream = T.stream('statuses/filter', { track:   hashtags });
  console.log("Started tweet stream for hashtag #" + JSON.stringify(hashtags));

  tweetStream.on('connected', function (response) {
    console.log("Stream connected to twitter for #" + JSON.stringify(hashtags));
  })
  tweetStream.on('error', function (error) {
    console.log("Error in Stream for #" + JSON.stringify(hashtags) + " " + error);
  })
  tweetStream.on('tweet', function (tweet) {
    produceTweetEvent(tweet);
  });
}

const kafka = require('kafka-node');
const APP_NAME ="TwitterConsumer"

var EVENT_HUB_PUBLIC_IP = process.env.KAFKA_HOST ||'129.150.77.116';
var TOPIC_NAME = process.env.KAFKA_TOPIC ||'a516817-tweetstopic';
// var EVENT_HUB_PUBLIC_IP = process.env.KAFKA_HOST ||'192.168.188.102';
// var TOPIC_NAME = process.env.KAFKA_TOPIC ||'tweets-topic';
var ZOOKEEPER_PORT = process.env.ZOOKEEPER_PORT ||2181;

var Producer = kafka.Producer;
var client = new kafka.Client(EVENT_HUB_PUBLIC_IP );
var producer = new Producer(client);
KeyedMessage = kafka.KeyedMessage;

producer.on('ready', function () {
  console.log("Producer is ready in " + APP_NAME);
});
producer.on('error', function (err) {
  console.log("failed to create the client or the producer " + JSON.stringify(err));
})


let payloads = [
  { topic: TOPIC_NAME, messages: '*', partition: 0 }
];

function produceTweetEvent(tweet) {
      // find out which of the original hashtags { track: ['oraclecode', 'java', 'devoxxUK'] } in the hashtags for this tweet; 
    //that is the one for the tagFilter property
    // select one other hashtag from tweet.entities.hashtags to set in property hashtag
    var tagFilter="oraclecode";
    var extraHashTag="liveForCode";
    for (var i = 0; i < tweet.entities.hashtags.length; i++) {
      var tag = tweet.entities.hashtags[i].text.toLowerCase();
      console.log("inspect hashtag "+tag);
      var idx = tracks.track.indexOf(tag);
      if (idx > -1) {
        tagFilter = tag;
      } else {
        extraHashTag = tag
      }
    }//for


    var tweetEvent = {
      "eventType": "tweetEvent"
      , "text": tweet.text
      , "isARetweet": tweet.retweeted_status ? "y" : "n"
      , "author": tweet.user.name
      , "hashtag": extraHashTag
      , "createdAt": tweet.created_at
      , "language": tweet.lang
      , "tweetId": tweet.id
      , "tagFilter": tagFilter
      , "originalTweetId": tweet.retweeted_status ? tweet.retweeted_status.id : null
    };
    eventPublisher.publishEvent(tweet.id,tweetEvent)
}

var eventPublisher = module.exports;


  eventPublisher.publishEvent = function (eventKey, event) {
    km = new KeyedMessage(eventKey, JSON.stringify(event));
    payloads = [
      { topic: TOPIC_NAME, messages: [km], partition: 0 }
    ];
    producer.send(payloads, function (err, data) {
      if (err) {
        console.error("Failed to publish event with key " + eventKey + " to topic " + TOPIC_NAME + " :" + JSON.stringify(err));
      }
      console.log("Published event with key " + eventKey + " to topic " + TOPIC_NAME + " :" + JSON.stringify(data));
    });
  
  }

