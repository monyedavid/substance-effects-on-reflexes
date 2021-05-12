import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkAggregator {
	val spark: SparkSession = SparkSession.builder()
		.appName("The Science project")
		.master("local[2]")
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	import spark.implicits._

	case class UserResponse(sessionId: String, clickDuration: Long)

	case class UserAverage(sessionId: String, avgDuration: Double)

	def readUserResponse(): Dataset[UserResponse] = spark
		.readStream
		.format("kafka")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("subscribe", "science")
		.load()
		.select("value") // SQL.DataFrame
		.as[String] // Dataset[String]
		.map { line =>
			val tokens = line.split(",")
			val sessionId = tokens(0)
			val time = tokens(1).toLong

			UserResponse(sessionId, time)
		}

	def updateUserResponseTime
	(n: Int)
	(sessionId: String, group: Iterator[UserResponse], state: GroupState[List[UserResponse]]): Iterator[UserAverage]
	= {
		group flatMap { record =>
			val lastWindow =
				if (state.exists) state.get
				else List()

			val windowLength = lastWindow.length
			val newWindow =
				if (windowLength >= n) lastWindow.tail :+ record
				else lastWindow :+ record

			// update state - spark to give access to the state on the next batch
			state.update(newWindow)

			if (newWindow.length >= n) { // compute new average
				val newAverage = newWindow.map(_.clickDuration).sum * 1.0 / n
				Iterator(UserAverage(sessionId, avgDuration = newAverage))
			} else Iterator()

		}
	}

	def averageResponseTime(n: Int): Unit =
		readUserResponse()
			.groupByKey(_.sessionId)
			.flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
			.writeStream
			.format("console")
			.outputMode("append")
			.start().awaitTermination()

	def logUserResponses(): Unit =
		readUserResponse()
			.writeStream
			.format("console")
			.outputMode("append")
			.start()
			.awaitTermination()


	def main(args: Array[String]): Unit = {
		averageResponseTime(3)
	}

}
