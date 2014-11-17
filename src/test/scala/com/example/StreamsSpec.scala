package com.example

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{FlowMaterializer, OverflowStrategy}
import org.reactivestreams.Subscription
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import rx.functions.Func1
import rx.internal.reactivestreams.RxSubscriberToRsSubscriberAdapter
import rx.schedulers.Schedulers
import rx.{Observable, RxReactiveStreams, Subscriber}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class StreamsSpec extends FunSuite with ScalaFutures {

    val client = new Client

    test("retrieveAll should return Reactive Streams Publisher, providing interop") {

        val p = Promise[Int]()
        val eventualNumberOfMessagesProcessed = p.future

        // one can use rxJava Subscription if one wishes to
        val rxSubscriber = new Subscriber[Integer]() {

            val counter = new AtomicInteger()

            override def onCompleted(): Unit = {
                println("on subscriber completed")
                p.success(counter.get())
            }

            override def onError(e: Throwable): Unit = {
                p.failure(e)
            }

            override def onNext(t: Integer): Unit = {
                println(s"${Thread.currentThread().getName} rx On next")
                counter.incrementAndGet()
            }
        }

        val publisher = client.retrieveAll()

        publisher.subscribe(new RxSubscriberToRsSubscriberAdapter[Integer](rxSubscriber))

        whenReady(eventualNumberOfMessagesProcessed, timeout(Span(400, Seconds))) { count =>
            assert(count == 300)
        }
    }

    test("retrieveAll stream processing using Flow and rs Subscription") {
        val publisher = client.retrieveAll()

        implicit val system = ActorSystem("heavy-computing")
        implicit val materializer = FlowMaterializer()

        import scala.concurrent.ExecutionContext.Implicits.global

        val source = Source(publisher)

        val flow = Flow[Integer].mapAsync {
            i => Future {
                println(s"${Thread.currentThread().getName} doing heavy calculation with $i")
                Thread.sleep(1000)
                i
            }
        }

        val p = Promise[Int]()
        val eventualNumberOfMessagesProcessed = p.future

        val sub = new org.reactivestreams.Subscriber[Integer]() {
            val counter = new AtomicInteger()
            var subscription: Subscription = null

            override def onSubscribe(s: Subscription): Unit = {
                subscription = s
                s.request(4)
            }

            override def onError(t: Throwable): Unit = p.failure(t)

            override def onComplete(): Unit = p.success(counter.get())

            override def onNext(i: Integer): Unit = {
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
        val publisher = client.retrieveAll()

        implicit val system = ActorSystem("heavy-computing")
        implicit val materializer = FlowMaterializer()

        import scala.concurrent.ExecutionContext.Implicits.global

        val source = Source(publisher)

        val counter = new AtomicInteger()

        val f = source mapAsync {
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

        val publisher = client.retrieveAll()

        val observable: Observable[Integer] = RxReactiveStreams.toObservable(publisher)

        val p = Promise[Int]()
        val eventualNumberOfMessagesProcessed = p.future

        import scala.concurrent.ExecutionContext.Implicits.global

        val rxSubscriber = new Subscriber[Integer]() {

            val counter = new AtomicInteger()

            override def onError(e: Throwable): Unit = p.failure(e)

            override def onCompleted(): Unit = p.success(counter.get())

            override def onNext(i: Integer): Unit = {
                counter.incrementAndGet()
                println(s"${Thread.currentThread().getName} I am done with $i")
            }
        }

        val o2: Observable[Future[Integer]] = observable.map(new Func1[Integer, Future[Integer]]() {
            override def call(i: Integer): Future[Integer] = Future {
                println(s"${Thread.currentThread().getName} doing heavy calculation with $i")
                Thread.sleep(1000)
                i
            }
        })

        o2.buffer(4).flatMap(new Func1[util.List[Future[Integer]], Observable[Integer]]() {
            override def call(list: util.List[Future[Integer]]): Observable[Integer] = {
                val results = Await.result(Future.sequence(list.asScala), 5.seconds).asJava
                Observable.from(results)
            }
        }).observeOn(Schedulers.computation()).onBackpressureBuffer().subscribe(rxSubscriber)

        whenReady(eventualNumberOfMessagesProcessed, timeout(Span(100, Seconds))) { count => {
            assert(count == 300)
        }}
    }
}
