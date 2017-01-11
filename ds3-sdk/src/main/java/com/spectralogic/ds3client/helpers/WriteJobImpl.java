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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.PutSequentialStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.OriginalChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.AggregatingChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.common.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.filterChunks;

class WriteJobImpl extends JobImpl {
    static private final Logger LOG = LoggerFactory.getLogger(WriteJobImpl.class);

    private final List<Objects> filteredChunks;
    private final ChecksumType.Type checksumType;

    private final int retryAfter; // Negative retryAfter value represent infinity retries
    private final int retryDelay; //Negative value means use default

    private Ds3ClientHelpers.MetadataAccess metadataAccess = null;
    private ChecksumFunction checksumFunction = null;

    public WriteJobImpl(
            final Ds3Client client,
            final MasterObjectList masterObjectList,
            final int retryAfter,
            final ChecksumType.Type type,
            final int objectTransferAttempts,
            final int retryDelay,
            final EventRunner eventRunner) {
        super(client, masterObjectList, objectTransferAttempts, eventRunner);
        this.filteredChunks = getChunks(masterObjectList);
        this.retryAfter = retryAfter;
        this.retryDelay = retryDelay;

        this.checksumType = type;
    }

    @Override
    public void attachMetadataReceivedListener(final MetadataReceivedListener listener) {
        throw new IllegalStateException("Metadata listeners are not used with Write jobs");
    }

    @Override
    public void removeMetadataReceivedListener(final MetadataReceivedListener listener) {
        throw new IllegalStateException("Metadata listeners are not used with Write jobs");
    }

    @Override
    public Ds3ClientHelpers.Job withMetadata(final Ds3ClientHelpers.MetadataAccess access) {
        checkRunning();
        this.metadataAccess = access;
        return this;
    }

    @Override
    public Ds3ClientHelpers.Job withChecksum(final ChecksumFunction checksumFunction) {
        this.checksumFunction = checksumFunction;
        return this;
    }

    @Override
    public void transfer(final ObjectChannelBuilder channelBuilder)
            throws IOException {
        try {
            running = true;
            LOG.debug("Starting job transfer");
            if (this.masterObjectList == null || this.masterObjectList.getObjects() == null) {
                LOG.info("There is nothing to transfer for job"
                        + (this.getJobId() == null ? "" : " " + this.getJobId().toString()));
                return;
            }

            final BlobStrategy blobStrategy = new PutSequentialStrategy(
                    client,
                    this.masterObjectList,
                    retryAfter,
                    retryDelay,
                    getEventDispatcher()
            );

            final ChannelStrategy channelStrategy = new AggregatingChannelStrategy(new OriginalChannelStrategy(channelBuilder));

            final TransferStrategyBuilder transferStrategyBuilder = new TransferStrategyBuilder()
                    .withBlobStrategy(blobStrategy)
                    .withChannelStrategy(channelStrategy)
                    .withBucketName(masterObjectList.getBucketName())
                    .withJobId(getJobId().toString())
                    // .withTransferRetryBehavior(new MaxNumObjectTransferAttemptsBehavior(getObjectTransferAttempts()))
                    .withJobPartTracker(getJobPartTracker())
                    .withEventDispatcher(getEventDispatcher())
                    .withChecksumType(checksumType)
                    .withChecksumFunction(checksumFunction);

            try (final JobState jobState = new JobState(
                    channelBuilder,
                    filteredChunks,
                    getJobPartTracker(),
                    ImmutableMap.<String, ImmutableMultimap<BulkObject, Range>>of())) {
                try (final TransferStrategy transferStrategy = transferStrategyBuilder.makePutSequentialTransferStrategy()) {
                    while (jobState.hasObjects()) {
                        transferStrategy.transfer();
                    }
                }

            } catch (final IOException | RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } catch (final Throwable t) {
            emitFailureEvent(makeFailureEvent(FailureEvent.FailureActivity.PuttingObject, t, masterObjectList.getObjects().get(0)));
            throw t;
        }
    }

    @Override
    protected List<Objects> getChunks(final MasterObjectList masterObjectList) {
        if (masterObjectList == null || masterObjectList.getObjects() == null) {
            LOG.info("Job has no data to transfer");
            return null;
        }

        LOG.info("Ready to start transfer for job {} with {} chunks", masterObjectList.getJobId().toString(), masterObjectList.getObjects().size());
        return filterChunks(masterObjectList.getObjects());
    }

    @Override
    protected JobPartTrackerDecorator makeJobPartTracker(final List<Objects> chunks, final EventRunner eventRunner) {
        if (chunks == null) {
            return null;
        }

        final JobPartTrackerDecorator result = new JobPartTrackerDecorator(chunks, eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                getEventDispatcher().emitObjectCompletedEvent(name);
            }
        });

        return result;
    }
}

