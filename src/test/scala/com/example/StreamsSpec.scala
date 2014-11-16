package com.example

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.scaladsl._
import akka.stream.{FlowMaterializer, MaterializerSettings, OverflowStrategy}
import org.reactivestreams.{Publisher, Subscription}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import rx.functions.Func1
import rx.internal.reactivestreams.RxSubscriberToRsSubscriberAdapter
import rx.{Observable, RxReactiveStreams, Subscriber}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class StreamsSpec extends FunSuite with ScalaFutures {

    test("retrieveAll should return Reactive Streams Publisher, providing interop") {

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

    test("retrieveAll stream processing using Flow and rs Subscription") {
        val publisher = retrieveAll()

        implicit val system = ActorSystem("heavy-computing")
        implicit val materializer = FlowMaterializer()

        import scala.concurrent.ExecutionContext.Implicits.global

        val source = Source(publisher)

        val flow = Flow[Int].mapAsync {
            i => Future {
                println(s"${Thread.currentThread().getName} doing heavy calculation with $i")
                Thread.sleep(1000)
                i
            }
        }

        val p = Promise[Int]()
        val eventualNumberOfMessagesProcessed = p.future

        val sub = new org.reactivestreams.Subscriber[Int]() {
            val counter = new AtomicInteger()
            var subscription: Subscription = null

            override def onSubscribe(s: Subscription): Unit = {
                subscription = s
                s.request(4)
            }

            override def onError(t: Throwable): Unit = p.failure(t)

            override def onComplete(): Unit = p.success(counter.get())

            override def onNext(i: Int): Unit = {
                counter.incrementAndGet()
                println(s"I am done with $i")
                subscription.request(4)
            }
        }

        flow.runWith(source, SubscriberSink(sub))

        whenReady(eventualNumberOfMessagesProcessed, timeout(Span(100, Seconds))) { count => {
            assert(count == 300)
            system.shutdown()
        }}
    }

    test("retrieveAll stream processing using Source transformations") {
        val publisher = retrieveAll()

        implicit val system = ActorSystem("heavy-computing")
        implicit val materializer = FlowMaterializer()

        import scala.concurrent.ExecutionContext.Implicits.global

        val source = Source(publisher)

        val counter = new AtomicInteger()

        val f =source mapAsync {
            i => Future {
                println(s"${Thread.currentThread().getName} doing heavy calculation with $i")
                Thread.sleep(1000)
                i
            }
        } buffer(4, OverflowStrategy.backpressure) foreach {
            i => {
                counter.incrementAndGet()
                println(s"I am done with $i")
            }
        }

        whenReady(f, timeout(Span(100, Seconds))) { count => {
            assert(counter.get() == 300)
            system.shutdown()
        }}
    }

    test("retrieveAll stream processing using RxJava transformations") {

        val publisher = retrieveAll()

        val observable: Observable[Int] = RxReactiveStreams.toObservable(publisher)

        val p = Promise[Int]()
        val eventualNumberOfMessagesProcessed = p.future

        implicit val execContext = ExecutionContexts.fromExecutor(Executors.newFixedThreadPool(4))

        val rxSubscriber = new Subscriber[util.List[Future[Int]]]() {

            val counter = new AtomicInteger()

            override def onError(e: Throwable): Unit = p.failure(e)

            override def onCompleted(): Unit = p.success(counter.get())

            override def onNext(list: util.List[Future[Int]]): Unit = {

                val results = Await.result(Future.sequence(list.asScala), 5.seconds)

                results.foreach {
                    i => {
                        counter.incrementAndGet()
                        println(s"I am done with $i")
                    }
                }
            }
        }

        val o2: Observable[Future[Int]] = observable.map(new Func1[Int, Future[Int]]() {
            override def call(i: Int): Future[Int] = Future {
                println(s"${Thread.currentThread().getName} doing heavy calculation with $i")
                Thread.sleep(1000)
                i
            }
        })

        o2.buffer(4).subscribe(rxSubscriber)

        whenReady(eventualNumberOfMessagesProcessed, timeout(Span(100, Seconds))) { count => {
            assert(count == 300)
        }}
    }

    private def retrieveAll(): Publisher[Int] = {

        implicit val system = ActorSystem("rs")
        implicit val materializer = FlowMaterializer(MaterializerSettings(system).withDispatcher("my-thread-pool-dispatcher"))

        val sourceOne = Source((1 to 100).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceTwo = Source((101 to 200).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})
        val sourceThree = Source((201 to 300).toList).map(i => {println(s"${Thread.currentThread().getName} read $i"); i})

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
