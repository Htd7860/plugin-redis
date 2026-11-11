package io.kestra.plugin.redis.list;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.redis.AbstractRedisConnection;
import io.kestra.plugin.redis.RedisConnectionInterface;
import io.kestra.plugin.redis.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Realtime trigger from a Redis list",
    description = "Continuously polls `LPOP` one element at a time (every ~100 ms) and starts one Execution per item. Cancellation or shutdown closes the active connection and stops polling; an already sent pop cannot be rolled back. Use [Trigger](https://kestra.io/plugins/plugin-redis/triggers/io.kestra.plugin.redis.list.trigger) for batched polling."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume an element from the head of a list in real-time.",
            full = true,
            code = """
                id: list_listen
                namespace: company.team

                tasks:
                  - id: echo
                    type: io.kestra.plugin.core.log.Log
                    message: "Received '{{ trigger.value }}'"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.redis.list.RealtimeTrigger
                    url: redis://localhost:6379/0
                    key: mytriggerkey
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output>, ListPopBaseInterface, RedisConnectionInterface {
    private Property<String> url;

    private Property<String> key;

    @Schema(
        title = "Format of the data contained in Redis",
        description = SERDE_TYPE_DESCRIPTION
    )
    @Builder.Default
    @NotNull
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<CountDownLatch> waitForTermination = new AtomicReference<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<AbstractRedisConnection.RedisFactory> factoryRef = new AtomicReference<>();

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        ListPop task = ListPop.builder()
            .url(this.url)
            .key(this.key)
            .count(Property.ofValue(1))
            .serdeType(this.serdeType)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map((record) -> TriggerService.generateRealtimeExecution(this, conditionContext, context, Output.of(record)));
    }

    public Publisher<Object> publisher(final ListPop task,
        final RunContext runContext) {
        return Flux.create(
            fluxSink ->
            {
                var termination = new CountDownLatch(1);
                waitForTermination.set(termination);

                try {
                    if (!isActive.get() || fluxSink.isCancelled()) {
                        return;
                    }

                    try (var factory = task.redisFactory(runContext)) {
                        factoryRef.set(factory);
                        fluxSink.onCancel(factory::closeAsync);

                        String rKey = runContext.render(this.key).as(String.class).orElseThrow();
                        var rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(SerdeType.STRING);

                        while (isActive.get() && !fluxSink.isCancelled()) {
                            factory.getSyncCommands().lpop(rKey, 1)
                                .forEach(s -> fluxSink.next(ListPop.deserialize(runContext, rKey, rSerdeType, s)));
                            if (!isActive.get() || fluxSink.isCancelled()) {
                                break;
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                isActive.set(false);
                            }
                        }
                    } finally {
                        factoryRef.set(null);
                    }
                } catch (Exception e) {
                    if (isActive.get() && !fluxSink.isCancelled()) {
                        fluxSink.error(e);
                    }
                } finally {
                    termination.countDown();
                    fluxSink.complete();
                }
            }
        );
    }

    @Builder
    @Getter
    @AllArgsConstructor(staticName = "of")
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The value"
        )
        private Object value;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        isActive.set(false);
        var factory = factoryRef.get();
        if (factory != null) {
            factory.closeAsync();
        }
        var termination = waitForTermination.get();
        if (wait && termination != null) {
            try {
                // Opening a connection cannot be interrupted through factoryRef until it is published.
                termination.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
