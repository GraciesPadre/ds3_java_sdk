package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ThrottlingStrategy_Test {
    @Test
    public void testThatCompletionEventsFire() {
        final List<Integer> itemsToProcess = new ArrayList<>();
        itemsToProcess.add(1);

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);
        final AtomicBoolean gotProducerCompletedEvent = new AtomicBoolean(false);
        final AtomicBoolean gotConsumerCompletedEvent = new AtomicBoolean(false);

        final ThrottlingStrategy transferThrottlingStrategy = makeBlockingThrottlingStrategy(
                new LinkedBlockingQueue<Integer>(itemsToProcess.size()),
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent);

        testThatCompletionEventsFire(transferThrottlingStrategy, gotProducerCompletedEvent, gotConsumerCompletedEvent);
    }

    private void testThatCompletionEventsFire(final ThrottlingStrategy throttlingStrategy,
                                              final AtomicBoolean gotProducerCompletedEvent,
                                              final AtomicBoolean gotConsumerCompletedEvent)
    {
        throttlingStrategy.run();

        try {
            for (int i = 0; i < 10; ++i) {
                if (gotProducerCompletedEvent.get() && gotConsumerCompletedEvent.get()) {
                    break;
                }

                Thread.sleep(100);
            }
        } catch (final InterruptedException e) {
            fail("Waiting for transfer complete got interrupted.");
        }

        assertTrue(gotProducerCompletedEvent.get());
        assertTrue(gotConsumerCompletedEvent.get());
    }

    private ThrottlingStrategy makeBlockingThrottlingStrategy(
            final BlockingQueue<Integer> blockingQueue,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final AtomicBoolean gotProducerCompletedEvent,
            final AtomicBoolean gotConsumerCompletedEvent)
    {
        return makeThrottlingStrategyBuilder(
                blockingQueue,
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent)
                .buildBlockingQueueThrottlingStrategy();
    }

    @SuppressWarnings("unchecked")
    private TransferThrottlingStrategyBuilder<Integer> makeThrottlingStrategyBuilder(final BlockingQueue<Integer> blockingQueue,
                                                                                     final Iterable<Integer> itemsToProcess,
                                                                                     final AtomicInteger numItemsProcessed,
                                                                                     final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
                                                                                     final AtomicBoolean gotProducerCompletedEvent,
                                                                                     final AtomicBoolean gotConsumerCompletedEvent)
    {
        return TransferThrottlingStrategyBuilder.<Integer>builder()
                .withBlockingQueue(blockingQueue)
                .withConsumerExecutorService(Executors.newSingleThreadExecutor())
                .withProducerExecutorService(Executors.newSingleThreadExecutor())
                .withItemsToProcess(itemsToProcess)
                .withConsumerRunnable(new ConsumerRunnable<Integer>() {
                    @Override
                    public void run(final Integer nextQueuedValue) {
                        if (nextQueuedValue != numItemsProcessed.get()) {
                            allWorkQueueItemsEqualItemsToProcess.set(false);
                        }

                        numItemsProcessed.incrementAndGet();
                    }
                })
                .withProducerTransferCompleteHandler(new TransferCompleteHandler() {
                    @Override
                    public void onTransferComplete() {
                        gotProducerCompletedEvent.set(true);
                    }
                })
                .withConsumerTransferCompleteHandler(new TransferCompleteHandler() {
                    @Override
                    public void onTransferComplete() {
                        gotConsumerCompletedEvent.set(true);
                    }
                });
    }

    @Test
    public void testThatCompletionEventsFireWithNonBlockingQueue() {
        final List<Integer> itemsToProcess = new ArrayList<>();
        itemsToProcess.add(1);

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);
        final AtomicBoolean gotProducerCompletedEvent = new AtomicBoolean(false);
        final AtomicBoolean gotConsumerCompletedEvent = new AtomicBoolean(false);

        final ThrottlingStrategy transferThrottlingStrategy = makeNonBlockingThrottlingStrategy(
                null,
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent);

        testThatCompletionEventsFire(transferThrottlingStrategy, gotProducerCompletedEvent, gotConsumerCompletedEvent);
    }

    private ThrottlingStrategy makeNonBlockingThrottlingStrategy(
            final BlockingQueue<Integer> blockingQueue,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final AtomicBoolean gotProducerCompletedEvent,
            final AtomicBoolean gotConsumerCompletedEvent)
    {
        return makeThrottlingStrategyBuilder(
                blockingQueue,
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent)
                .buildNonBlockingQueueThrottlingStrategy();
    }

    @Test
    public void testThatCompletionEventsFireWhenWeCancelTransfer() {
        final int numItemsToProcess = 1000000;
        final int workQueueSize = 10000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);
        final AtomicBoolean gotProducerCompletedEvent = new AtomicBoolean(false);
        final AtomicBoolean gotConsumerCompletedEvent = new AtomicBoolean(false);

        final TransferThrottlingStrategy transferThrottlingStrategy = makeBlockingCancellableStrategy(
                workQueueSize,
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent);

        testThatCompletionEventsFireWhenWeCancelTransfer(transferThrottlingStrategy,
                gotProducerCompletedEvent, gotConsumerCompletedEvent, numItemsProcessed, numItemsToProcess);
    }

    private void testThatCompletionEventsFireWhenWeCancelTransfer(final TransferThrottlingStrategy transferThrottlingStrategy,
                                                                  final AtomicBoolean gotProducerCompletedEvent,
                                                                  final AtomicBoolean gotConsumerCompletedEvent,
                                                                  final AtomicInteger numItemsProcessed,
                                                                  final int numItemsToProcess)
    {
        transferThrottlingStrategy.run();

        try {
            for (int i = 0; i < 10; ++i) {
                if (gotProducerCompletedEvent.get() && gotConsumerCompletedEvent.get()) {
                    break;
                }

                Thread.sleep(100);
            }
        } catch (final InterruptedException e) {
            fail("Waiting for transfer complete got interrupted.");
        }

        assertTrue(gotProducerCompletedEvent.get());
        assertTrue(gotConsumerCompletedEvent.get());
        assertTrue(numItemsProcessed.get() < numItemsToProcess);
    }

    private TransferThrottlingStrategy makeBlockingCancellableStrategy(
            final int workQueueSize,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final AtomicBoolean gotProducerCompletedEvent,
            final AtomicBoolean gotConsumerCompletedEvent)
    {
        final TransferThrottlingStrategy transferThrottlingStrategy = makeCancellableStrategyBuilder(
                workQueueSize, itemsToProcess, numItemsProcessed, allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent, gotConsumerCompletedEvent)
                .buildBlockingQueueThrottlingStrategy();

        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(new Runnable() {
                                              @Override
                                              public void run() {
                                                  transferThrottlingStrategy.stop();

                                                  scheduledExecutorService.shutdown();
                                              }
                                          },
                1, TimeUnit.SECONDS);

        return transferThrottlingStrategy;
    }

    @Test
    public void testThatCompletionEventsFireWhenWeCancelNonBlockingTransfer() {
        final int numItemsToProcess = 1000000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);
        final AtomicBoolean gotProducerCompletedEvent = new AtomicBoolean(false);
        final AtomicBoolean gotConsumerCompletedEvent = new AtomicBoolean(false);

        final TransferThrottlingStrategy transferThrottlingStrategy = makeNonBlockingCancellableStrategy(
                itemsToProcess,
                numItemsProcessed,
                allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent,
                gotConsumerCompletedEvent);

        testThatCompletionEventsFireWhenWeCancelTransfer(transferThrottlingStrategy,
                gotProducerCompletedEvent, gotConsumerCompletedEvent, numItemsProcessed, numItemsToProcess);
    }

    private TransferThrottlingStrategy makeNonBlockingCancellableStrategy(
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final AtomicBoolean gotProducerCompletedEvent,
            final AtomicBoolean gotConsumerCompletedEvent)
    {
        final TransferThrottlingStrategy transferThrottlingStrategy = makeCancellableStrategyBuilder(
                0, itemsToProcess, numItemsProcessed, allWorkQueueItemsEqualItemsToProcess,
                gotProducerCompletedEvent, gotConsumerCompletedEvent)
                .buildNonBlockingQueueThrottlingStrategy();

        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(new Runnable() {
                                              @Override
                                              public void run() {
                                                  transferThrottlingStrategy.stop();

                                                  scheduledExecutorService.shutdown();
                                              }
                                          },
                1, TimeUnit.SECONDS);

        return transferThrottlingStrategy;
    }

    @SuppressWarnings("unchecked")
    private TransferThrottlingStrategyBuilder<Integer> makeCancellableStrategyBuilder(
            final int workQueueSize,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final AtomicBoolean gotProducerCompletedEvent,
            final AtomicBoolean gotConsumerCompletedEvent)
    {
        final TransferThrottlingStrategyBuilder<Integer> builder = TransferThrottlingStrategyBuilder.<Integer>builder()
                .withConsumerExecutorService(Executors.newSingleThreadExecutor())
                .withProducerExecutorService(Executors.newSingleThreadExecutor())
                .withItemsToProcess(itemsToProcess)
                .withConsumerRunnable(new ConsumerRunnable<Integer>() {
                    @Override
                    public void run(final Integer nextQueuedValue) {
                        if (nextQueuedValue != numItemsProcessed.get()) {
                            allWorkQueueItemsEqualItemsToProcess.set(false);
                        }

                        numItemsProcessed.incrementAndGet();

                        try {
                            Thread.sleep(100);
                        } catch (final InterruptedException e) {
                            fail("Waiting for transfer complete got interrupted.");
                        }
                    }
                })
                .withProducerTransferCompleteHandler(new TransferCompleteHandler() {
                    @Override
                    public void onTransferComplete() {
                        gotProducerCompletedEvent.set(true);
                    }
                })
                .withConsumerTransferCompleteHandler(new TransferCompleteHandler() {
                    @Override
                    public void onTransferComplete() {
                        gotConsumerCompletedEvent.set(true);
                    }
                });

        if (workQueueSize > 0) {
            builder.withBlockingQueue(new LinkedBlockingQueue<Integer>(workQueueSize));
        }

        return builder;
    }

    @Test
    public void testSingleThreadedExecutor() {
        final int numItemsToProcess = 1000000;
        final int workQueueSize = 10000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);

        @SuppressWarnings("unchecked")
        final TransferThrottlingStrategy transferThrottlingStrategy = makeSingleThreadedThrottlingStrategyBuilder(
                workQueueSize, itemsToProcess, numItemsProcessed, allWorkQueueItemsEqualItemsToProcess, numItemsToProcess)
                .buildBlockingQueueThrottlingStrategy();

        transferThrottlingStrategy.run();
    }

    @SuppressWarnings("unchecked")
    private TransferThrottlingStrategyBuilder<Integer> makeSingleThreadedThrottlingStrategyBuilder(
            final int workQueueSize,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final AtomicBoolean allWorkQueueItemsEqualItemsToProcess,
            final int numItemsToProcess)
    {
        final TransferThrottlingStrategyBuilder<Integer> transferThrottlingStrategyBuilder =
                TransferThrottlingStrategyBuilder.<Integer>builder()

                .withConsumerExecutorService(Executors.newSingleThreadExecutor())
                .withProducerExecutorService(Executors.newSingleThreadExecutor())
                .withItemsToProcess(itemsToProcess)
                .withConsumerRunnable(new ConsumerRunnable<Integer>() {
                    @Override
                    public void run(final Integer nextQueuedValue) {
                        if (nextQueuedValue.intValue() != numItemsProcessed.get()) {
                            allWorkQueueItemsEqualItemsToProcess.set(false);
                        }

                        numItemsProcessed.incrementAndGet();
                    }
                })
                .withConsumerTransferCompleteHandler(new TransferCompleteHandler() {
                    @Override
                    public void onTransferComplete() {
                        assertEquals(numItemsToProcess, numItemsProcessed.get());
                        assertTrue(allWorkQueueItemsEqualItemsToProcess.get());
                    }
                });

        if (workQueueSize > 0) {
            transferThrottlingStrategyBuilder.withBlockingQueue(new LinkedBlockingQueue<Integer>(workQueueSize));
        }

        return transferThrottlingStrategyBuilder;
    }

    @Test
    public void testSingleThreadedExecutorWithNonBlockingQueue() {
        final int numItemsToProcess = 1000000;
        final int workQueueSize = 10000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);
        final AtomicBoolean allWorkQueueItemsEqualItemsToProcess = new AtomicBoolean(true);

        final TransferThrottlingStrategy transferThrottlingStrategy = makeSingleThreadedThrottlingStrategyBuilder(
                0, itemsToProcess, numItemsProcessed, allWorkQueueItemsEqualItemsToProcess, numItemsToProcess)
                .buildNonBlockingQueueThrottlingStrategy();

        transferThrottlingStrategy.run();
    }

    @Test
    public void testMultiThreadedExecutor() {
        final int numItemsToProcess = 1000000;
        final int workQueueSize = 10000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);

        @SuppressWarnings("unchecked")
        final TransferThrottlingStrategy transferThrottlingStrategy = makeMultiThreadedThrottlingStrategyBuilder(
                workQueueSize, itemsToProcess, numItemsProcessed, numItemsToProcess)
                .buildBlockingQueueThrottlingStrategy();

        transferThrottlingStrategy.run();
    }

    @SuppressWarnings("unchecked")
    private TransferThrottlingStrategyBuilder<Integer> makeMultiThreadedThrottlingStrategyBuilder(
            final int workQueueSize,
            final Iterable<Integer> itemsToProcess,
            final AtomicInteger numItemsProcessed,
            final int numItemsToProcess)
    {
        final TransferThrottlingStrategyBuilder<Integer> transferThrottlingStrategyBuilder =
                TransferThrottlingStrategyBuilder.<Integer>builder()
                        .withConsumerExecutorService(Executors.newFixedThreadPool(5))
                        .withProducerExecutorService(Executors.newFixedThreadPool(5))
                        .withItemsToProcess(itemsToProcess)
                        .withConsumerRunnable(new ConsumerRunnable<Integer>() {
                            @Override
                            public void run(final Integer nextQueuedValue) {
                                numItemsProcessed.incrementAndGet();
                            }
                        })
                        .withConsumerTransferCompleteHandler(new TransferCompleteHandler() {
                            @Override
                            public void onTransferComplete() {
                                assertEquals(numItemsToProcess, numItemsProcessed.get());
                            }
                        });

        if (workQueueSize > 0) {
            transferThrottlingStrategyBuilder.withBlockingQueue(new LinkedBlockingQueue<Integer>(workQueueSize));
        }

        return transferThrottlingStrategyBuilder;
    }

    @Test
    public void testMultiThreadedExecutorWithNonBlockingQueue() {
        final int numItemsToProcess = 1000000;

        final List<Integer> itemsToProcess = new ArrayList<>(numItemsToProcess);

        for (int i = 0; i < numItemsToProcess; ++i) {
            itemsToProcess.add(i);
        }

        final AtomicInteger numItemsProcessed = new AtomicInteger(0);

        @SuppressWarnings("unchecked")
        final TransferThrottlingStrategy transferThrottlingStrategy = makeMultiThreadedThrottlingStrategyBuilder(
                0, itemsToProcess, numItemsProcessed, numItemsToProcess)
                .buildNonBlockingQueueThrottlingStrategy();

        transferThrottlingStrategy.run();
    }
}
