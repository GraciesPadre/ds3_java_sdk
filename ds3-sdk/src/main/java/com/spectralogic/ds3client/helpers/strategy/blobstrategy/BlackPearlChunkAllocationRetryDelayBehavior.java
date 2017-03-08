package com.spectralogic.ds3client.helpers.strategy.blobstrategy;

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlackPearlChunkAllocationRetryDelayBehavior implements ChunkAllocationRetryDelayBehavior {
    private static final Logger LOG = LoggerFactory.getLogger(BlackPearlChunkAllocationRetryDelayBehavior.class);

    private int delayIntervalInSeconds;
    private final EventDispatcher eventDispatcher;

    public BlackPearlChunkAllocationRetryDelayBehavior(final EventDispatcher eventDispatcher) {
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");

        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void delay(final int delayIntervalInSeconds) throws InterruptedException {
        LOG.debug("Will retry allocate chunk call after {} seconds", delayIntervalInSeconds);

        this.delayIntervalInSeconds = delayIntervalInSeconds;

        eventDispatcher.emitWaitingForChunksEvents(delayIntervalInSeconds);

        Thread.sleep(delayIntervalInSeconds * 1000);
    }

    @Override
    public int getDelayIntervalInSeconds() {
        return delayIntervalInSeconds;
    }
}
