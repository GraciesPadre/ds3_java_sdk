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

package com.spectralogic.ds3client.helpers;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.Job;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.models.MasterObjectList;
import com.spectralogic.ds3client.models.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

abstract class JobImpl implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(JobImpl.class);

    protected final Ds3Client client;
    protected final MasterObjectList masterObjectList;
    protected boolean running = false;

    private final EventDispatcher eventDispatcher;

    private final TransferStrategyBuilder transferStrategyBuilder;

    public JobImpl(final TransferStrategyBuilder transferStrategyBuilder,
                   final Ds3Client client,
                   final MasterObjectList masterObjectList,
                   final EventDispatcher eventDispatcher)
    {
        this.transferStrategyBuilder = transferStrategyBuilder;
        this.client = client;
        this.masterObjectList = masterObjectList;
        this.eventDispatcher = eventDispatcher;
    }
    
    @Override
    public UUID getJobId() {
        if (this.masterObjectList == null) {
            return null;
        }
        return this.masterObjectList.getJobId();
    }

    @Override
    public String getBucketName() {
        if (this.masterObjectList == null) {
            return null;
        }
        return this.masterObjectList.getBucketName();
    }
    
    @Override
    public Job withMaxParallelRequests(final int maxParallelRequests) {
        transferStrategyBuilder.withNumConcurrentTransferThreads(maxParallelRequests);
        return this;
    }

    protected void checkRunning() {
        if (running) throw new IllegalStateException("You cannot modify a job after calling transfer");
    }

    @Override
    public void attachChecksumListener(final ChecksumListener listener) {
        checkRunning();
        eventDispatcher.attachChecksumListener(listener);
    }

    @Override
    public void removeChecksumListener(final ChecksumListener listener) {
        checkRunning();
        eventDispatcher.removeChecksumListener(listener);
    }

    @Override
    public void attachWaitingForChunksListener(final WaitingForChunksListener listener) {
        checkRunning();
        eventDispatcher.attachWaitingForChunksListener(listener);
    }

    @Override
    public void removeWaitingForChunksListener(final WaitingForChunksListener listener) {
        checkRunning();
        eventDispatcher.removeWaitingForChunksListener(listener);
    }

    @Override
    public void attachFailureEventListener(final FailureEventListener listener) {
        checkRunning();
        eventDispatcher.attachFailureEventListener(listener);
    }

    @Override
    public void removeFailureEventListener(final FailureEventListener listener) {
        checkRunning();
        eventDispatcher.removeFailureEventListener(listener);
    }

    @Override
    public void attachDataTransferredListener(final DataTransferredListener listener) {
        checkRunning();
        eventDispatcher.attachDataTransferredListener(listener);
    }

    @Override
    public void removeDataTransferredListener(final DataTransferredListener listener) {
        checkRunning();
        eventDispatcher.removeDataTransferredListener(listener);
    }

    @Override
    public void attachObjectCompletedListener(final ObjectCompletedListener listener) {
        checkRunning();
        eventDispatcher.attachObjectCompletedListener(listener);
    }

    @Override
    public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
        checkRunning();
        eventDispatcher.removeObjectCompletedListener(listener);
    }

    @Override
    public void transfer(final Ds3ClientHelpers.ObjectChannelBuilder channelBuilder) throws IOException {
        transferStrategyBuilder.withChannelBuilder(channelBuilder);
    }

    protected TransferStrategyBuilder transferStrategyBuilder() {
        return transferStrategyBuilder;
    }

    protected void emitFailureEvent(final FailureEvent failureEvent) {
        eventDispatcher.emitFailureEvent(failureEvent);
    }

    protected FailureEvent makeFailureEvent(final FailureEvent.FailureActivity failureActivity,
                                            final Throwable causalException,
                                            final Objects chunk)
    {
        return new FailureEvent.Builder()
                .doingWhat(failureActivity)
                .withCausalException(causalException)
                .withObjectNamed(getLabelForChunk(chunk))
                .usingSystemWithEndpoint(client.getConnectionDetails().getEndpoint())
                .build();
    }

    protected String getLabelForChunk(final Objects chunk) {
        try {
            return chunk.getObjects().get(0).getName();
        } catch (final Throwable t) {
            LOG.error("Failed to get label for chunk.", t);
        }

        return "unnamed object";
    }

    protected EventDispatcher getEventDispatcher() {
        return eventDispatcher;
    }
}
