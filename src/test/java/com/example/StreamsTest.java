package com.example;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StreamsTest {

    Client client = new Client();

    @Test
    public void retrieveAllStreamProcessingUsingReactorTransformations() throws Exception {

        Publisher<Integer> publisher = client.retrieveAll();

        Stream<Integer> integerStream = Streams.create(publisher);

        CompletableFuture<Integer> eventualItemsCount = new CompletableFuture<>();

        Subscriber<Integer> subscriber = new Subscriber<Integer>() {

            private AtomicInteger counter = new AtomicInteger();
            private Subscription subscription = null;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(4);
            }

            @Override
            public void onNext(Integer integer) {
                counter.incrementAndGet();
                System.out.println("I am done with " + integer);
                subscription.request(4);
            }

            @Override
            public void onError(Throwable throwable) {
                eventualItemsCount.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                eventualItemsCount.complete(counter.get());
            }
        };

        integerStream.parallel(4,
            stream -> stream.map(
                i -> {
                    System.out.println(Thread.currentThread().getName() + " expensive computation on " + i);
                    sleep(1000);
                    return i;
                }
            )
        ).subscribe(subscriber);


        Integer count = eventualItemsCount.get();
        assertThat(count, is(300));

    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
