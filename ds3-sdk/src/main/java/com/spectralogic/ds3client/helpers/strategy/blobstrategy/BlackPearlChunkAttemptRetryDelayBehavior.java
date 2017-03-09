package com.spectralogic.ds3client.helpers.strategy.blobstrategy;

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlackPearlChunkAttemptRetryDelayBehavior implements ChunkAttemptRetryDelayBehavior {
    private static final Logger LOG = LoggerFactory.getLogger(BlackPearlChunkAttemptRetryDelayBehavior.class);

    private final EventDispatcher eventDispatcher;

    public BlackPearlChunkAttemptRetryDelayBehavior(final EventDispatcher eventDispatcher) {
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");

        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void delay(final int delayIntervalInSeconds) throws InterruptedException {
        LOG.debug("Will retry allocate chunk call after {} seconds", delayIntervalInSeconds);

        eventDispatcher.emitWaitingForChunksEvents(delayIntervalInSeconds);

        Thread.sleep(delayIntervalInSeconds * 1000);
    }
}
