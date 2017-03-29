package com.spectralogic.ds3client.helpers.strategy.blobstrategy;

import com.spectralogic.ds3client.exceptions.Ds3NoMoreRetriesException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class MaxChunkAttemptsRetryBehavior implements ChunkAttemptRetryBehavior {
    private final int maxAttempts;
    private final AtomicInteger currentAttempt = new AtomicInteger(0);

    public MaxChunkAttemptsRetryBehavior(final int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Override
    public void invoke() throws IOException {
        if (currentAttempt.incrementAndGet() > maxAttempts) {
            throw new Ds3NoMoreRetriesException(maxAttempts);
        }
    }

    @Override
    public void reset() {
        currentAttempt.set(0);
    }
}
