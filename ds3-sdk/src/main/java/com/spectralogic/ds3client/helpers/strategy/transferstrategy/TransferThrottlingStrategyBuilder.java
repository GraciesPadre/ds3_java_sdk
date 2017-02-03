/*
 * ****************************************************************************
 *    Copyright 2014-2016 Spectra Logic Corporation. All Rights Reserved.
 *    Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *    this file except in compliance with the License. A copy of the License is located at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file.
 *    This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *    CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *    specific language governing permissions and limitations under the License.
 *  ****************************************************************************
 */

package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class TransferThrottlingStrategyBuilder<QueueContentType> {
    private BlockingQueue<QueueContentType> blockingQueue;
    private ExecutorService producerExecutor;
    private ExecutorService consumerExecutor;
    private Iterable<QueueContentType> itemsToProcess;
    private ConsumerRunnable<QueueContentType> consumerRunnable;
    private TransferCompleteHandler producerTransferCompleteHandler;
    private TransferCompleteHandler consumerTransferCompleteHandler;

    public TransferThrottlingStrategyBuilder withBlockingQueue(final BlockingQueue<QueueContentType> blockingQueue) {
        this.blockingQueue = blockingQueue;
        return this;
    }

    public TransferThrottlingStrategyBuilder withProducerExecutorService(final ExecutorService executorService) {
        this.producerExecutor = executorService;
        return this;
    }

    public TransferThrottlingStrategyBuilder withConsumerExecutorService(final ExecutorService executorService) {
        this.consumerExecutor = executorService;
        return this;
    }

    public TransferThrottlingStrategyBuilder withItemsToProcess(final Iterable<QueueContentType> itemsToProcess) {
        this.itemsToProcess = itemsToProcess;
        return this;
    }

    public TransferThrottlingStrategyBuilder withConsumerRunnable(final ConsumerRunnable<QueueContentType> consumerRunnable) {
        this.consumerRunnable = consumerRunnable;
        return this;
    }

    public TransferThrottlingStrategyBuilder withProducerTransferCompleteHandler(final TransferCompleteHandler transferCompleteHandler) {
        this.producerTransferCompleteHandler = transferCompleteHandler;
        return this;
    }

    public TransferThrottlingStrategyBuilder withConsumerTransferCompleteHandler(final TransferCompleteHandler transferCompleteHandler) {
        this.consumerTransferCompleteHandler = transferCompleteHandler;
        return this;
    }

    public static TransferThrottlingStrategyBuilder builder() {
        return new TransferThrottlingStrategyBuilder();
    }

    public TransferThrottlingStrategy buildBlockingQueueThrottlingStrategy() {
        Preconditions.checkNotNull(blockingQueue, "blockingQueue may not be null.");
        return makeThrottlingStrategy(new WorkQueueImpl<>(blockingQueue));
    }

    private TransferThrottlingStrategy makeThrottlingStrategy(final WorkQueue<QueueContentType> workQueue) {
        Preconditions.checkNotNull(producerExecutor, "producerExecutor may not be null.");
        Preconditions.checkNotNull(consumerExecutor, "consumerExecutor may not be null.");
        Preconditions.checkNotNull(itemsToProcess, "itemsToProcess may not be null.");
        Preconditions.checkNotNull(consumerRunnable, "consumerRunnable may not be null");

        if (producerTransferCompleteHandler == null) {
            producerTransferCompleteHandler = new NullTransferCompleteHandler();
        }

        if (consumerTransferCompleteHandler == null) {
            consumerTransferCompleteHandler = new NullTransferCompleteHandler();
        }

        final  ImmutablePair<ProducerQueueThrottlingStrategy<QueueContentType>, ConsumerQueueThrottlingStrategy<QueueContentType>> producerConsumerThrottlers =
                makeProducerConsumerThrottlers(workQueue);
        return new TransferThrottlingStrategy(producerConsumerThrottlers.getLeft(), producerConsumerThrottlers.getRight());
    }

    private ImmutablePair<ProducerQueueThrottlingStrategy<QueueContentType>, ConsumerQueueThrottlingStrategy<QueueContentType>> makeProducerConsumerThrottlers(final WorkQueue<QueueContentType> workQueue) {
        return ImmutablePair.of(makeProducerQueueThrottlingStrategy(workQueue), makeConsumerQueueThrottlingStrategy(workQueue));
    }

    private ProducerQueueThrottlingStrategy<QueueContentType> makeProducerQueueThrottlingStrategy(final WorkQueue<QueueContentType> workQueue) {
        return new ProducerQueueThrottlingStrategy<>(workQueue, producerExecutor, itemsToProcess, producerTransferCompleteHandler);
    }

    private ConsumerQueueThrottlingStrategy<QueueContentType> makeConsumerQueueThrottlingStrategy(final WorkQueue<QueueContentType> workQueue) {
        return new ConsumerQueueThrottlingStrategy<>(workQueue, consumerExecutor, Iterables.size(itemsToProcess), consumerRunnable, consumerTransferCompleteHandler);
    }

    public TransferThrottlingStrategy buildNonBlockingQueueThrottlingStrategy() {
        return makeThrottlingStrategy(new NonBlockingWorkQueue<>(itemsToProcess));
    }
}
