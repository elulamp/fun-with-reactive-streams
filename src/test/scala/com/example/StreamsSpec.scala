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

    test("retrieveAll should returnd Reactive Streams Publisher, providing interop") {

        val p = Promise[Int]()

        val eventualNumberOfMessagesProcessed = p.future

        // one can use rxJava Subscription if one wishes to
        val rxSubscriber = new Subscriber[Int]() {

            val counter = new AtomicInteger()

            override def onCompleted(): Unit = {
                println("on subscriber completed")
                p.success(counter.get())
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
            assert(count == 30)
        }
    }

    private def retrieveAll(): Publisher[Int] = {

        implicit val system = ActorSystem("rs")
        implicit val materializer = FlowMaterializer(MaterializerSettings(system).withDispatcher("my-thread-pool-dispatcher"))

        val sourceOne = Source((1 to 10).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceTwo = Source((11 to 20).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceThree = Source((21 to 30).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})

        val publisherSink = PublisherSink[Int]()
        val onCompletionSink = OnCompleteSink[Int]{
            _ => {
                println("on complition")
                system.shutdown()
            }
        }

        val materialized = FlowGraph { implicit builder =>
            import akka.stream.scaladsl.FlowGraphImplicits._

            val merge = Merge[Int]
            val broadcast = Broadcast[Int]

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
