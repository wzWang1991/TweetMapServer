TweetMapServer
==============
##Overview
TweetMap is an app that shows the coordinates of Tweets and the heatmap of them. It supports Real Time Mode, in which mode you can see the latest tweets.

Demo: <http://tweetmap.elasticbeanstalk.com/>

##Usage
### Normal Mode
1. Pick an time range. You can use the slider to choose which period you are interested in.
2. Choose the key word you like.
3. Click button "Scatter", "Tweets", "HeatMap" to get different views. You can check the description under the buttons.

### Real Time Mode
Click "Switch to Real Time Mode" next to the time range slider. Click "Go!" and wait for the latest tweets, which will be shown as markers on the map. You can click the markers to see the text of each tweet.

## Design
### Tweets Collection
We use com.twitter.hbc package to help us use Twitter Stream APIs to get the tweets related to some topics. We only pick the tweets that have coordinate information because we need to shown them on the map.

### Tweets Database
We use RDS to save the tweets. The class Rds provides method to insert the data and get the data using a key word and time range.

### Servlet
Servlet is used to process the post request and return the data in json format.

### Real Time Mode Server
This is a websocket server, on which we also use a com.twitter.hbc to get the stream of tweets. It will send the tweets to all clients that are connecting to the server.

For details, you can visit <https://github.com/wzWang1991/TweetMapRealTimeServer>.

### Front End
**Normal Mode**: We use Bootstrap framework to help us build the front end. Google Map is used to display the map and show the markers and heat map. Ajax is used to communicate with servlet.

**Real Time Mode**: We use websocket to communicate with real time mode server. When we get a new tweet, we will display it as a marker on the map with animation.

