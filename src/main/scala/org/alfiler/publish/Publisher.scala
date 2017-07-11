package org.alfiler.publish

import java.time.Instant
import java.util.Properties

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import org.apache.kafka.clients.producer.ProducerRecord

case object Start

case object End

case object Message

private object Publisher {
  def props(): Props = Props(new Publisher)
}

private class Publisher extends Actor {

  import org.apache.kafka.clients.producer.KafkaProducer
  import collection.JavaConverters._
  import scala.concurrent.duration._

  val props = new Properties()
  props.putAll(Map("bootstrap.servers" -> "localhost:9092",
    "acks" -> "all",
    "retries" -> 0,
    "batch.size" -> 16384,
    "linger.ms" -> 1,
    "buffer.memory" -> 33554432,
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
    .asInstanceOf[Map[String, AnyRef]].asJava)

  val producer = new KafkaProducer[String, String](props)

  def messageCreator() = {

    new ProducerRecord[String, String]("test", s"mensaje a enviar ${Instant.now()}")
  }

  var scheduledTask:Option[Cancellable] = None

  override def receive: Receive = {
    case Start =>
      scheduledTask = Some(context.system.scheduler.schedule(FiniteDuration(1, SECONDS), FiniteDuration(1, SECONDS)) {
      1 to 100000 foreach (_ => self ! Message)
    }(context.dispatcher))
    case Message => producer.send(messageCreator())
    case End =>
      scheduledTask.foreach(_.cancel())
      producer.close()
      context.stop(self)
  }
}

object PublisherHandler {
  def props() = Props(new PublisherHandler())
}

class PublisherHandler extends Actor {


  var publisher: Option[ActorRef] = None

  override def receive: Receive = {
    case Start => publisher = Some(context.actorOf(Publisher.props()))
      publisher.foreach(_ ! Start)
    case End => publisher.foreach(_ ! End)
  }
}
