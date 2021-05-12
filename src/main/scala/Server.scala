import java.util.Properties
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object Server {

	implicit val system: ActorSystem = ActorSystem()
	implicit val materializer: ActorMaterializer = ActorMaterializer()

	val KafkaTopic = "science"
	val KafkaBootstrapServer = "localhost:9092"

	def getProducer: KafkaProducer[Long, String] = {
		val props = new Properties()
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootstrapServer)
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

		new KafkaProducer[Long, String](props)
	}


	def getRoute(producer: KafkaProducer[Long, String]): Route = {
		pathEndOrSingleSlash { // dir.route => "/"
			get {
				complete(
					HttpEntity(
						ContentTypes.`text/html(UTF-8)`,
						Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
					)
				)
			}
		} ~
			path("api" / "report") {
				(parameter("sessionId".as[String]) & parameter("time".as[Long])) { (sessionId: String, time: Long) =>
					println(s"[found]: $sessionId & $time")

					// send records to kafka-server
					// key in ProducerRecord():=> ensures data arrives in same partition (ordering)
					val record = new ProducerRecord[Long, String](KafkaTopic, 0, s"$sessionId,$time")
					producer.send(record)
					producer.flush() // send buffered data, close kafka producer
					complete(StatusCodes.OK)
				}
			}

	}


	def main(args: Array[String]): Unit = {
		val kafkaProducer = getProducer
		val bindingFuture = Http().bindAndHandle(getRoute(kafkaProducer), "localhost", 6969)
		bindingFuture.foreach { binding =>
			binding.whenTerminated.onComplete(_ => kafkaProducer.close())
		}
	}

}
