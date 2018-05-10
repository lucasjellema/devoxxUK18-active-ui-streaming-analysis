// Handle REST requests (POST and GET) for departments
var express = require('express') //npm install express
  , bodyParser = require('body-parser') // npm install body-parser
  , fs = require('fs')
  , https = require('https')
  , http = require('http')
  , request = require('request');

//var logger = require("./logger.js");
var tweetListener = require("./tweetListener.js");
var tweetAnalyticsListener = require("./tweetAnalyticsListener");
var tweetLikesAnalyticsListener = require("./tweetLikesAnalyticsListener");
var tweetLikeProducer = require("./tweetLikeProducer.js");
var sseMW = require('./sse');
var PORT = process.env.APP_PORT || 8104;

const app = express()
  .use(bodyParser.urlencoded({ extended: true }))
  //configure sseMW.sseMiddleware as function to get a stab at incoming requests, in this case by adding a Connection property to the request
  .use(sseMW.sseMiddleware)
  .use(express.static(__dirname + '/public'))
  .get('/updates', function (req, res) {
    console.log("res (should have sseConnection)= " + res.sseConnection);
    var sseConnection = res.sseConnection;
    console.log("sseConnection= ");
    sseConnection.setup();
    sseClients.add(sseConnection);
  });

const server = http.createServer(app);

const WebSocket = require('ws');
// create WebSocket Server
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  console.log('WebSocket Client connected');
  ws.on('close', () => console.log('Client disconnected'));

  ws.on('message', function incoming(message) {
    if (message.indexOf("tweetLike") > -1) {
      var tweetLike = JSON.parse(message);
        var likedTweet = tweetCache[tweetLike.tweetId];
      if (likedTweet) {
        updateWSClients(JSON.stringify({ "eventType": "tweetLiked", "likedTweet": likedTweet }));
        tweetLikeProducer.produceTweetLike(likedTweet);
      }
    }
  });
});

server.listen(PORT, function listening() {
  console.log('Listening on %d', server.address().port);
});
setInterval(() => {
  updateWSClients(JSON.stringify({ "eventType": "time", "time": new Date().toTimeString() }));
}, 1000);

function updateWSClients(message) {
  wss.clients.forEach((client) => {
    client.send(message);
  });

}

// Realtime updates
var sseClients = new sseMW.Topic();

updateSseClients = function (message) {
  sseClients.forEach(function (sseConnection) {
    sseConnection.send(message);
  }
    , this // this second argument to forEach is the thisArg (https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/forEach) 
  );
}



console.log('server running on port 3000');

// heartbeat
setInterval(() => {
  updateSseClients({ "eventType": "tweetEvent", "text": "Heartbeat: " + new Date() + "  #oraclecode ", "isARetweet": "N", "author": "Your Node backend system", "hashtag": "HEARTBEAT", "createdAt": null, "language": "en", "tweetId": "1492545590100001b1Un", "tagFilter": "oraclecode", "originalTweetId": null })
}
  , 2500000
)
var tweetCache = {};
tweetListener.subscribeToTweets((message) => {
  var tweetEvent = JSON.parse(message);
  tweetCache[tweetEvent.tweetId] = tweetEvent;
  updateSseClients(tweetEvent);
})

tweetAnalyticsListener.subscribeToTweetAnalytics((message) => {
  console.log("tweet analytic " + message);
  var tweetAnalyticsEvent = JSON.parse(message);
  console.log("tweetAnalyticsEvent " + JSON.stringify(tweetAnalyticsEvent));
  updateSseClients(tweetAnalyticsEvent);
})

var tweetLikesTopN = { "windowTimestamp":0}
var nEquals = 3;

const tweetLikeAggregationPeriod = 30; //in seconds
setInterval(()=> {console.log("Resetting TweetLikes Aggregations ");       
   tweetLikesTopN= {windowTimestamp : new Date()}
}, tweetLikeAggregationPeriod * 1000)

//TODO: periodically wipe out all like analytics
tweetLikesAnalyticsListener.subscribeToTweetLikeAnalytics((message) => {
  console.log("tweetLikes analytic " + message);
  var tweetLikesAnalyticsEvent = JSON.parse(message);
  //{"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1492900954165001X6eF","conference":"oow17","count":22,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},null]}
  //tweetLikes analytic {"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1492900954165001X6eF","conference":"oow17","count":22,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},null]}
  // enrich tweetLikesAnalytic - add tweet text and author
  console.log("tweet analytic " + message);
  var tweetLikesAnalyticsEvent = JSON.parse(message);
  console.log("tweetAnalyticsEvent " + JSON.stringify(tweetLikesAnalyticsEvent));
  var tweet = tweetCache[tweetLikesAnalyticsEvent.tweetId];
  if (tweet){
    var windowTimestamp = tweetLikesAnalyticsEvent.windowTimestamp;
    if (windowTimestamp>tweetLikesTopN.windowTimestamp) {
      // reset tweetLikesTopN
      // new time window - it feels like we should reset the top N matrix (or at least the section for the current tag)
      console.log("&&&&&&&&DINDONG - new time slot!")
      tweetLikesTopN.windowTimestamp = windowTimestamp
    }
   var tag = tweet.tagFilter
    if (!tweetLikesTopN[tag]) {
      tweetLikesTopN[tag] = []; //top N array
      tweetLikesTopN[tag][0] = {"tweetId":tweet.tweetId,"count":tweetLikesAnalyticsEvent.count}
    } else {
      // check if tweetId already occurs in array; if so, update the entry and resort the array
      var found = false;
      tweetLikesTopN[tag].forEach(function(entry){ if (entry.tweetId ==tweet.tweetId) {entry.count = tweetLikesAnalyticsEvent.count;found = true;} })
      // if not, put it on N+1th position, sort the array and slice off the last element
      if (!found) {
        tweetLikesTopN[tag][tweetLikesTopN[tag].length] = {"tweetId":tweet.tweetId,"count":tweetLikesAnalyticsEvent.count}
      }//
      // sort
      tweetLikesTopN[tag].sort(function (a,b) {return b.count - a.count})
      // slice off to no more than nEquals
      tweetLikesTopN[tag] = tweetLikesTopN[tag].slice(0,nEquals)
    }
console.log("==================== Top N")
console.log(JSON.stringify(tweetLikesTopN))

    sseTweetLikesAnalyticsEvent = {"nrs":[]}
    //{"tweetId":tweetLikesAnalyticsEvent.tweetId,"conference":tweet.tagFilter,"count":tweetLikesAnalyticsEvent.count,"window":tweetLikesAnalyticsEvent.windowTimestamp}
//  ,{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},{"tweetId":"1497393952355001DjRn","conference":"oow17","count":18,"window":null},null]}
  sseTweetLikesAnalyticsEvent.eventType = "tweetLikesAnalytics";
  sseTweetLikesAnalyticsEvent.conference = tweet.tagFilter;
  for (var i = 0; i < tweetLikesTopN[tag].length; i++) {
    sseTweetLikesAnalyticsEvent.nrs[i]= {}
    // get tweet from local cache
      var tweetId = tweetLikesTopN[tag][i].tweetId;
      console.log("tweet id = " + tweetId);
      sseTweetLikesAnalyticsEvent.nrs[i].tweetId = tweetId;
      sseTweetLikesAnalyticsEvent.nrs[i].conference = tag;
      sseTweetLikesAnalyticsEvent.nrs[i].count = tweetLikesTopN[tag][i].count;
      var tweet = tweetCache[tweetId];
      if (tweet) {
        sseTweetLikesAnalyticsEvent.nrs[i].text = tweet.text;
        sseTweetLikesAnalyticsEvent.nrs[i].author = tweet.author;
      }//if tweet
    }//for


  console.log("tweetLikesAnalyticsEvent " + JSON.stringify(sseTweetLikesAnalyticsEvent));
  updateSseClients(sseTweetLikesAnalyticsEvent);
}//tweet
})