package com.example

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{FlowMaterializer, MaterializerSettings}
import org.reactivestreams.Publisher

class Client {

    def retrieveAll(): Publisher[Integer] = {

        implicit val system = ActorSystem("rs")
        implicit val materializer = FlowMaterializer(MaterializerSettings(system).withDispatcher("my-thread-pool-dispatcher"))

        val sources =
            List(
                Source((1 to 100).toList.map(i => i: Integer).iterator),
                Source((101 to 200).toList.map(i => i: Integer).iterator),
                Source((201 to 300).toList.map(i => i: Integer).iterator))

        val publisherSink = PublisherSink[Integer]()
        val onCompletionSink = OnCompleteSink[Integer] {
            _ => {
                println("on complition")
                system.shutdown()
            }
        }

        val materialized = FlowGraph { implicit builder =>
            import akka.stream.scaladsl.FlowGraphImplicits._

            val merge = Merge[Integer]
            val broadcast = Broadcast[Integer]

            sources.foreach(source => source ~> merge)

            merge ~> broadcast
            broadcast ~> publisherSink
            broadcast ~> onCompletionSink

        }.run()

        materialized.get(publisherSink)
    }

}
