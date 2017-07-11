package org.alfiler.publish

import akka.actor.ActorSystem

/**
  * Created by aroa on 11/07/17.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val as = ActorSystem("kafka-jander")


    val actor = as.actorOf(PublisherHandler.props())

    actor ! Start

    val actor2 = as.actorOf(PublisherHandler.props())

    actor2 ! Start

    scala.io.StdIn.readLine()
    actor ! End
    actor2 ! End
    as.terminate()
  }
}
