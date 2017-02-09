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
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.ChecksumFunction;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategyMaker;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.GetSequentialBlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.PutSequentialBlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.AggregatingChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.OriginalChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.MasterObjectList;
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
    private int numTransferRetries;
    private int numChunkAllocationRetries;
    private int retryDelayInSeconds;
    private Ds3Client ds3Client;
    private MasterObjectList masterObjectList;
    private Ds3ClientHelpers.ObjectChannelBuilder channelBuilder;

    private static final int MAX_CONCURRENT_TRANSFER_THREADS = 10;

    public TransferStrategyBuilder withChannelStrategy(final ChannelStrategy channelStrategy) {
        this.channelStrategy = channelStrategy;
        return this;
    }

    public TransferStrategyBuilder withChannelBuilder(final Ds3ClientHelpers.ObjectChannelBuilder channelBuilder) {
        this.channelBuilder = channelBuilder;
        return this;
    }

    public TransferStrategyBuilder withTransferRetryBehavior(final TransferRetryBehavior transferRetryBehavior) {
        this.transferRetryBehavior = transferRetryBehavior;
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

    public TransferStrategyBuilder withNumTransferRetries(final int numRetries) {
        this.numTransferRetries = numRetries;
        return this;
    }

    public TransferStrategyBuilder withNumChunkAllocationRetries(final int numChunkAllocationRetries) {
        this.numChunkAllocationRetries = numChunkAllocationRetries;
        return this;
    }

    public TransferStrategyBuilder withRetryDelayInSeconds(final int retryDelayInSeconds) {
        this.retryDelayInSeconds = retryDelayInSeconds;
        return this;
    }

    public TransferStrategyBuilder withDs3Client(final Ds3Client ds3Client) {
        this.ds3Client = ds3Client;
        return this;
    }

    public TransferStrategyBuilder withMasterObjectList(final MasterObjectList masterObjectList) {
        this.masterObjectList = masterObjectList;
        return this;
    }

    /*
    public TransferStrategyBuilder withThrottlingStrategy(final ThrottlingStrategy throttlingStrategy) {
        this.throttlingStrategy = throttlingStrategy;
        return this;
    }
    */

    // TODO: implement these
    // public TransferStrategy makePutSequentialTransferStrategy() { }
    // public TransferStrategy makePutRandomTransferStrategy() { }

    public TransferStrategy makeOriginalSdkSemanticsPutTransferStrategy() {
        return makeTransferStrategy(
                new BlobStrategyMaker() {
                    @Override
                    public BlobStrategy makeBlobStrategy(final Ds3Client client,
                                                         final MasterObjectList masterObjectList,
                                                         final int retryAfter,
                                                         final int retryDelay,
                                                         final EventDispatcher eventDispatcher) {
                        return new PutSequentialBlobStrategy(ds3Client,
                                masterObjectList,
                                numChunkAllocationRetries,
                                retryDelayInSeconds,
                                eventDispatcher);
                    }
                },
                new TransferMethodMaker() {
                    @Override
                    public TransferMethod makeTransferMethod() {
                        return makePutTransferMethod();
                    }
                });
    }

    private TransferStrategy makeTransferStrategy(final BlobStrategyMaker blobStrategyMaker,
                                                  final TransferMethodMaker transferMethodMaker)
    {
        Preconditions.checkNotNull(ds3Client, "ds3Client may not be null.");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");
        Preconditions.checkNotNull(jobPartTracker);
        Preconditions.checkNotNull(masterObjectList, "masterObjectList may not be null.");
        Preconditions.checkNotNull(channelBuilder, "channelBuilder my not be null");
        Preconditions.checkNotNull(blobStrategyMaker, "blobStrategyMaker may not be null.");
        Preconditions.checkNotNull(transferMethodMaker, "transferMethodMaker may not be null.");

        bucketName = masterObjectList.getBucketName();
        Guard.throwOnNullOrEmptyString(bucketName, "bucketName may not be null or empty.");

        jobId = masterObjectList.getJobId().toString();
        Guard.throwOnNullOrEmptyString(jobId, "jobId may not be null or empty.");

        blobStrategy = blobStrategyMaker.makeBlobStrategy(ds3Client,
                masterObjectList,
                numChunkAllocationRetries,
                retryDelayInSeconds,
                eventDispatcher);

        eventDispatcher.attachBlobTransferredEventObserver(new BlobTransferredEventObserver(new UpdateStrategy<BulkObject>() {
            @Override
            public void update(final BulkObject eventData) {
                jobPartTracker.completePart(eventData.getName(), new ObjectPart(eventData.getOffset(), eventData.getLength()));
            }
        }));

        channelStrategy = new AggregatingChannelStrategy(new OriginalChannelStrategy(channelBuilder));

        final TransferMethod transferMethod = transferMethodMaker.makeTransferMethod();

        final SequentialTransferStrategy sequentialTransferStrategy = new SequentialTransferStrategy(blobStrategy);

        return sequentialTransferStrategy.withTransferMethod(transferMethod);
    }

    private TransferMethod makePutTransferMethod() {
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
            makeDefaultChecksumFunction();
        }
    }

    private void makeDefaultChecksumFunction() {
        final ChecksumFunction newChecksumFunction = new ChecksumFunction() {
            @Override
            public String compute(final BulkObject obj, final ByteChannel channel) {
                String checksum = null;

                try
                {
                    final InputStream dataStream = new SeekableByteChannelInputStream(channelStrategy.acquireChannelForBlob(obj));

                    dataStream.mark(Integer.MAX_VALUE);

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

    public TransferStrategy makeOriginalSdkSemanticsGetTransferStrategy() {
        return makeTransferStrategy(
                new BlobStrategyMaker() {
                    @Override
                    public BlobStrategy makeBlobStrategy(final Ds3Client client,
                                                         final MasterObjectList masterObjectList,
                                                         final int retryAfter,
                                                         final int retryDelay,
                                                         final EventDispatcher eventDispatcher) {
                        return new GetSequentialBlobStrategy(ds3Client,
                                masterObjectList,
                                numChunkAllocationRetries,
                                retryDelayInSeconds,
                                eventDispatcher);
                    }
                },
                new TransferMethodMaker() {
                    @Override
                    public TransferMethod makeTransferMethod() {
                        return makeGetTransferMethod();
                    }
                });
    }

    private TransferMethod makeGetTransferMethod() {
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker may not be null.");

        final TransferMethod transferMethod = new GetJobTransferMethod(channelStrategy,
                bucketName, jobId, eventDispatcher);

        if (transferRetryBehavior != null) {
            return transferRetryBehavior.wrap(transferMethod);
        }

        return transferMethod;
    }
}
