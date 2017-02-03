package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import java.util.concurrent.BlockingQueue;

public class WorkQueueImpl<QueueContentType> implements WorkQueue<QueueContentType> {
    private final BlockingQueue<QueueContentType> blockingQueue;

    public WorkQueueImpl(final BlockingQueue<QueueContentType> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void put(final QueueContentType item) throws InterruptedException {
        blockingQueue.put(item);
    }

    @Override
    public QueueContentType take() throws InterruptedException {
        return blockingQueue.take();
    }
}
