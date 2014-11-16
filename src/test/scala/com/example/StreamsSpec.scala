package com.example

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{FlowMaterializer, MaterializerSettings}
import org.reactivestreams.Publisher
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import rx.Subscriber
import rx.internal.reactivestreams.RxSubscriberToRsSubscriberAdapter

import scala.concurrent.Promise

class StreamsSpec extends FunSuite with ScalaFutures {

    implicit val system = ActorSystem("rs")

    test("retrieveAll should returnd Reactive Streams Publisher, providing interop") {

        val p = Promise[Int]()

        val eventualNumberOfMessagesProcessed = p.future

        // one can use rxJava Subscription if one wishes to
        val rxSubscriber = new Subscriber[Int]() {

            val counter = new AtomicInteger()

            override def onCompleted(): Unit = {
                p.success(counter.get())
                system.shutdown()
                system.awaitTermination()
            }

            override def onError(e: Throwable): Unit = {
                p.failure(e)
            }

            override def onNext(t: Int): Unit = {
                Thread.sleep(1000)
                println(s"${Thread.currentThread().getName} rx On next")
                counter.incrementAndGet()
            }
        }

        val publisher = retrieveAll()

        publisher.subscribe(new RxSubscriberToRsSubscriberAdapter[Int](rxSubscriber))

        whenReady(eventualNumberOfMessagesProcessed, timeout(Span(400, Seconds))) { count =>
            assert(count == 300)
        }
    }

    private def retrieveAll(): Publisher[Int] = {

        implicit val materializer = FlowMaterializer(MaterializerSettings(system).withDispatcher("my-thread-pool-dispatcher"))

        val sourceOne = Source((1 to 100).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceTwo = Source((101 to 200).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceThree = Source((201 to 300).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})

        val publisherSink = PublisherSink[Int]()

        val materialized = FlowGraph { implicit builder =>
            import akka.stream.scaladsl.FlowGraphImplicits._

            val merge = Merge[Int]

            sourceOne ~> merge
            sourceTwo ~> merge
            sourceThree ~> merge
            merge ~> publisherSink

        }.run()

        materialized.get(publisherSink)
    }

}
