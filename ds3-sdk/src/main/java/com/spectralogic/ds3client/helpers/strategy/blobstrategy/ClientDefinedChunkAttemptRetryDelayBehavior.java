package com.spectralogic.ds3client.helpers.strategy.blobstrategy;

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientDefinedChunkAttemptRetryDelayBehavior implements ChunkAttemptRetryDelayBehavior {
    private static final Logger LOG = LoggerFactory.getLogger(ClientDefinedChunkAttemptRetryDelayBehavior.class);

    private final int clientDefinedDelayInSeconds;
    private final EventDispatcher eventDispatcher;

    public ClientDefinedChunkAttemptRetryDelayBehavior(final int clientDefinedDelayInSeconds,
                                                       final EventDispatcher eventDispatcher)
    {
        Preconditions.checkState(clientDefinedDelayInSeconds >= 0, "clientDefinedDelayInSeconds must be >= 0");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null");

        this.clientDefinedDelayInSeconds = clientDefinedDelayInSeconds;
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void delay(final int delayIntervalInSeconds) throws InterruptedException {
        LOG.debug("Will retry allocate chunk call after {} seconds", clientDefinedDelayInSeconds);

        // Even though the BP is telling us how long to wait before trying to allocate a chunk again,
        // we'll use the delay time the client has specified.
        eventDispatcher.emitWaitingForChunksEvents(clientDefinedDelayInSeconds);

        Thread.sleep(clientDefinedDelayInSeconds * 1000);
    }
}
