package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

public interface WorkQueue<QueueContentType> {
    void put(final QueueContentType item) throws InterruptedException;
    QueueContentType take() throws InterruptedException;
}
