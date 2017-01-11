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
import com.spectralogic.ds3client.helpers.ChecksumFunction;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.utils.Guard;
import com.spectralogic.ds3client.utils.SeekableByteChannelInputStream;
import com.spectralogic.ds3client.utils.hashing.ChecksumUtils;
import com.spectralogic.ds3client.utils.hashing.Hasher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;

public final class TransferStrategyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TransferStrategyBuilder.class);

    private BlobStrategy blobStrategy;
    private ChannelStrategy channelStrategy;
    private String bucketName;
    private String jobId;
    private TransferRetryBehavior transferRetryBehavior;
    private ChecksumFunction checksumFunction;
    private ChecksumType.Type checksumType = ChecksumType.Type.NONE;
    private EventDispatcher eventDispatcher;
    private JobPartTracker jobPartTracker;

    public TransferStrategyBuilder withBlobStrategy(final BlobStrategy blobStrategy) {
        this.blobStrategy = blobStrategy;
        return this;
    }

    public TransferStrategyBuilder withChannelStrategy(final ChannelStrategy channelStrategy) {
        this.channelStrategy = channelStrategy;
        return this;
    }

    public TransferStrategyBuilder withTransferRetryBehavior(final TransferRetryBehavior transferRetryBehavior) {
        this.transferRetryBehavior = transferRetryBehavior;
        return this;
    }

    public TransferStrategyBuilder withBucketName(final String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public TransferStrategyBuilder withJobId(final String jobId) {
        this.jobId = jobId;
        return this;
    }

    public TransferStrategyBuilder withChecksumFunction(final ChecksumFunction checksumFunction) {
        this.checksumFunction = checksumFunction;
        return this;
    }

    public TransferStrategyBuilder withChecksumType(final ChecksumType.Type checksumType) {
        this.checksumType = checksumType;
        return this;
    }

    public TransferStrategyBuilder withEventDispatcher(final EventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
        return this;
    }

    public TransferStrategyBuilder withJobPartTracker(final JobPartTracker jobPartTracker) {
        this.jobPartTracker = jobPartTracker;
        return this;
    }

    public TransferStrategy makePutSequentialTransferStrategy() {
        Preconditions.checkNotNull(blobStrategy, "blobStrategy may not be null.");
        Preconditions.checkNotNull(channelStrategy, "channelStrategy may not be null.");
        Guard.throwOnNullOrEmptyString(bucketName, "bucketName may not be null or empty.");
        Guard.throwOnNullOrEmptyString(jobId, "jobId may not be null or empty.");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");
        Preconditions.checkNotNull(jobPartTracker);

        eventDispatcher.attachBlobTransferredEventObserver(new BlobTransferredEventObserver(new UpdateStrategy<BulkObject>() {
            @Override
            public void update(final BulkObject eventData) {
                jobPartTracker.completePart(eventData.getName(), new ObjectPart(eventData.getOffset(), eventData.getLength()));
            }
        }));

        final PutSequentialTransferStrategy putSequentialTransferStrategy = new PutSequentialTransferStrategy(blobStrategy);

        return putSequentialTransferStrategy.withTransferMethod(makeTransferMethod(putSequentialTransferStrategy));
    }

    private TransferMethod makeTransferMethod(final TransferStrategy transferStrategy) {
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker may not be null.");

        if (checksumType != ChecksumType.Type.NONE) {
            maybeAddChecksumFunction();
        }

        final TransferMethod transferMethod = new PutJobTransferMethod(channelStrategy,
                bucketName, jobId, eventDispatcher, checksumFunction, checksumType);

        if (transferRetryBehavior != null) {
            return transferRetryBehavior.wrap(transferMethod);
        }

        return transferMethod;
    }

    private void maybeAddChecksumFunction() {
        if (checksumFunction == null) {
            final ChecksumFunction newChecksumFunction = new ChecksumFunction() {
                @Override
                public String compute(final BulkObject obj, final ByteChannel channel) {
                    String checksum = null;

                    try
                    {
                        final InputStream dataStream = new SeekableByteChannelInputStream(channelStrategy.acquireChannelForBlob(obj));

                        final Hasher hasher = ChecksumUtils.getHasher(checksumType);

                        checksum = ChecksumUtils.hashInputStream(hasher, dataStream);

                        LOG.info("Computed checksum for blob: {}", checksum);

                        dataStream.reset();
                    } catch (final IOException e) {
                        eventDispatcher.emitFailureEvent(FailureEvent.builder()
                                .withObjectNamed(obj.getName())
                                .withCausalException(e)
                                .usingSystemWithEndpoint(blobStrategy.getClient().getConnectionDetails().getEndpoint())
                                .doingWhat(FailureEvent.FailureActivity.ComputingChecksum)
                                .build());
                        LOG.error("Error computing checksum.", e);
                    }

                    return checksum;
                }
            };

            checksumFunction = newChecksumFunction;
        }
    }
}
