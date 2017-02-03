package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import java.util.Iterator;

public class NonBlockingWorkQueue<QueueContentType> implements WorkQueue<QueueContentType> {
    final Iterator<QueueContentType> iterator;

    public NonBlockingWorkQueue(final Iterable<QueueContentType> iterable) {
        this.iterator = iterable.iterator();
    }

    @Override
    public void put(final QueueContentType item) throws InterruptedException {
        // intentionally not implemented
    }

    @Override
    public QueueContentType take() throws InterruptedException {
        return iterator.next();
    }
}
