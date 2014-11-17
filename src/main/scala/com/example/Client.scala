package com.example

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{MaterializerSettings, FlowMaterializer}
import org.reactivestreams.Publisher

class Client {

    def retrieveAll(): Publisher[Integer] = {

        implicit val system = ActorSystem("rs")
        implicit val materializer = FlowMaterializer(MaterializerSettings(system).withDispatcher("my-thread-pool-dispatcher"))

        val sourceOne = Source((1 to 100).toList).map(i => {
            println(s"${Thread.currentThread().getName} read $i"); i: Integer
        })
        val sourceTwo = Source((101 to 200).toList).map(i => {
            println(s"${Thread.currentThread().getName} read $i"); i: Integer
        })
        val sourceThree = Source((201 to 300).toList).map(i => {
            println(s"${Thread.currentThread().getName} read $i"); i: Integer
        })

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

            sourceOne ~> merge
            sourceTwo ~> merge
            sourceThree ~> merge
            merge ~> broadcast
            broadcast ~> publisherSink
            broadcast ~> onCompletionSink

        }.run()

        materialized.get(publisherSink)
    }

}
