# Substance effects on reflexes

- the effects of alcohol/substances on reflexes & response times

The mechanism

- the test subject sees a red square on browser window, must click as fast as possible
- when test subject clicks on the square, it will reset randomly on the screen
- repeat * 100

The data gathering

- on every click, the deltaTime is sent to the web-server
- the web-server sends the deltaTime to streaming pipeline
- deltaTimes are aggregated on rolling average of the past 10 values

Rolling Averages

In statistics, a moving(rolling) average is a calculation to analyze data points by creating a series of averages of
different subsets of the full data set. It is also called a moving mean or rolling mean and is a type of finite impulse
response filter. Variations include: simple, and cumulative, or weighted forms

Stack

- HTML/JS interface
- akka HTTP Rest Server
- Kafka
- Spark structured streaming

**How to run**

- clone project with intellijIdea (it will handle sbt project build)
- create topic **science** in kafka
- run [Server](src/main/scala/Server.scala)
- run [Spark Aggregator](src/main/scala/SparkAggregator.scala)
- navigate to `localhost:6969` in browser, click on red Square (*100)
- the rolling averages for test subject deltaTime is printed on
  the [Spark Aggregator](src/main/scala/SparkAggregator.scala) console 