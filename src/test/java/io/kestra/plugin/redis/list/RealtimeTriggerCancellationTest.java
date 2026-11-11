package io.kestra.plugin.redis.list;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.redis.StallingProxy;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import jakarta.inject.Inject;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class RealtimeTriggerCancellationTest {
    private static final String REDIS_URI = "redis://:redis@localhost:6379/0";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void cancellationStopsPollingAndKeepsTheRemainingItems() throws Exception {
        var key = "realtime_cancel_" + IdUtils.create();
        var client = RedisClient.create(REDIS_URI);
        var trigger = trigger(REDIS_URI, key);
        var executor = Executors.newSingleThreadExecutor();
        var received = new CountDownLatch(1);

        try (var connection = client.connect()) {
            var commands = connection.sync();
            commands.rpush(key, "first", "second", "third");

            try {
                var result = executor.submit(
                    () -> Flux.from(trigger.publisher(task(REDIS_URI), runContextFactory.of(Map.of())))
                        .take(1)
                        .doOnNext(item -> received.countDown())
                        .collectList()
                        .block()
                );

                assertTrue(received.await(5, TimeUnit.SECONDS));
                try {
                    assertThat(result.get(5, TimeUnit.SECONDS), contains("first"));
                } catch (TimeoutException e) {
                    throw new AssertionError("Producer kept running after cancellation; remaining Redis items: " + commands.lrange(key, 0, -1), e);
                }
                assertThat(commands.lrange(key, 0, -1), contains("second", "third"));
            } finally {
                trigger.stop();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                commands.del(key);
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    void cancellationClosesAnInFlightRead() throws Exception {
        shutdownClosesAnInFlightRead("cancel");
    }

    @Test
    void stopClosesAnInFlightRead() throws Exception {
        shutdownClosesAnInFlightRead("stop");
    }

    @Test
    void killClosesAnInFlightRead() throws Exception {
        shutdownClosesAnInFlightRead("kill");
    }

    private void shutdownClosesAnInFlightRead(String shutdown) throws Exception {
        var key = "realtime_cancel_stalled_" + IdUtils.create();

        try (var proxy = StallingProxy.start("localhost", 6379, "LPOP")) {
            var uri = "redis://:redis@localhost:" + proxy.getPort() + "/0";
            var trigger = trigger(uri, key);
            var subscriber = new BaseSubscriber<Object>() {
            };
            var executor = Executors.newSingleThreadExecutor();
            var terminator = Executors.newSingleThreadExecutor();

            try {
                var result = executor.submit(
                    () -> Flux.from(trigger.publisher(task(uri), runContextFactory.of(Map.of())))
                        .subscribe(subscriber)
                );

                assertTrue(proxy.awaitStalled(Duration.ofSeconds(10)));
                terminator.submit(() ->
                {
                    switch (shutdown) {
                        case "cancel" -> subscriber.cancel();
                        case "stop" -> trigger.stop();
                        case "kill" -> trigger.kill();
                        default -> throw new IllegalArgumentException(shutdown);
                    }
                }).get(5, TimeUnit.SECONDS);

                result.get(5, TimeUnit.SECONDS);
                assertTrue(proxy.awaitDisconnected(Duration.ofSeconds(5)));
            } finally {
                trigger.stop();
                terminator.shutdownNow();
                executor.shutdownNow();
                assertTrue(terminator.awaitTermination(10, TimeUnit.SECONDS));
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void killBeforeSubscriptionReturnsWithoutWaiting() throws Exception {
        var trigger = trigger(REDIS_URI, "unused");
        var executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(trigger::kill).get(5, TimeUnit.SECONDS);
            var result = executor.submit(
                () -> Flux.from(trigger.publisher(task("not-a-redis-uri"), runContextFactory.of(Map.of())))
                    .collectList()
                    .block()
            );
            assertThat(result.get(5, TimeUnit.SECONDS), is(empty()));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void redisErrorsStillReachTheSubscriber() throws Exception {
        var key = "realtime_wrongtype_" + IdUtils.create();
        var client = RedisClient.create(REDIS_URI);
        var trigger = trigger(REDIS_URI, key);

        try (var connection = client.connect()) {
            var commands = connection.sync();
            commands.set(key, "not-a-list");

            try {
                var error = assertThrows(
                    RedisCommandExecutionException.class,
                    () -> Flux.from(trigger.publisher(task(REDIS_URI), runContextFactory.of(Map.of())))
                        .collectList()
                        .block(Duration.ofSeconds(10))
                );
                assertThat(error.getMessage(), containsString("WRONGTYPE"));
            } finally {
                commands.del(key);
            }
        } finally {
            client.shutdown();
        }
    }

    private RealtimeTrigger trigger(String uri, String key) {
        return RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .url(Property.ofValue(uri))
            .key(Property.ofValue(key))
            .build();
    }

    private ListPop task(String uri) {
        return ListPop.builder().url(Property.ofValue(uri)).build();
    }
}
