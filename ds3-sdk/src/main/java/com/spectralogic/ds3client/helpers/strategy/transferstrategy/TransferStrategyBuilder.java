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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.ChecksumFunction;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.JobPartTrackerFactory;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.events.SameThreadEventRunner;
import com.spectralogic.ds3client.helpers.strategy.StrategyUtils;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlackPearlChunkAttemptRetryDelayBehavior;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategyMaker;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.ChunkAttemptRetryDelayBehavior;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.ClientDefinedChunkAttemptRetryDelayBehavior;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.ContinueForeverChunkAttemptsRetryBehavior;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.GetSequentialBlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.MaxChunkAttemptsRetryBehavior;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.PutSequentialBlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.RetryBehavior;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.NullChannelPreparable;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.RandomAccessChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.TruncatingChannelPreparable;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.MasterObjectList;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.utils.Guard;
import com.spectralogic.ds3client.utils.SeekableByteChannelInputStream;
import com.spectralogic.ds3client.utils.hashing.ChecksumUtils;
import com.spectralogic.ds3client.utils.hashing.Hasher;
import com.spectralogic.ds3client.helpers.JobState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.util.ArrayList;
import java.util.List;

public final class TransferStrategyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TransferStrategyBuilder.class);

    private static final int DEFAULT_MAX_CONCURRENT_TRANSFER_THREADS = 10;
    public static final int DEFAULT_CHUNK_ATTEMPT_RETRY_INTERVAL = -1;
    public static final int DEFAULT_CHUNK_ATTEMPT_RETRY_ATTEMPTS = -1;
    public static final int DEFAULT_OBJECT_TRANSFER_ATTEMPTS = 5;

    private BlobStrategy blobStrategy;
    private ChannelStrategy channelStrategy;
    private String bucketName;
    private String jobId;
    private TransferRetryDecorator transferRetryDecorator = new MaxNumObjectTransferAttemptsDecorator(DEFAULT_OBJECT_TRANSFER_ATTEMPTS);
    private ChecksumFunction checksumFunction;
    private ChecksumType.Type checksumType = ChecksumType.Type.NONE;
    private EventRunner eventRunner = new SameThreadEventRunner();
    private EventDispatcher eventDispatcher = new EventDispatcherImpl(eventRunner);
    private JobPartTracker jobPartTracker;
    private JobState jobState;
    private int numTransferRetries = DEFAULT_OBJECT_TRANSFER_ATTEMPTS;
    private int numConcurrentTransferThreads = DEFAULT_MAX_CONCURRENT_TRANSFER_THREADS;
    private int numChunkAttemptRetries = DEFAULT_CHUNK_ATTEMPT_RETRY_ATTEMPTS;
    private int chunkRetryDelayInSeconds = DEFAULT_CHUNK_ATTEMPT_RETRY_INTERVAL;
    private Ds3Client ds3Client;
    private MasterObjectList masterObjectList;
    private Ds3ClientHelpers.ObjectChannelBuilder channelBuilder;
    private ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs;
    private Ds3ClientHelpers.MetadataAccess metadataAccess;
    private RetryBehavior chunkAttemptRetryBehavior;
    private ChunkAttemptRetryDelayBehavior chunkAttemptRetryDelayBehavior;


    public TransferStrategyBuilder withBlobStrategy(final BlobStrategy blobStrategy) {
        this.blobStrategy = blobStrategy;
        return this;
    }

    public TransferStrategyBuilder withChannelStrategy(final ChannelStrategy channelStrategy) {
        this.channelStrategy = channelStrategy;
        return this;
    }

    public TransferStrategyBuilder withChannelBuilder(final Ds3ClientHelpers.ObjectChannelBuilder channelBuilder) {
        this.channelBuilder = channelBuilder;
        return this;
    }

    public TransferStrategyBuilder withTransferRetryDecorator(final TransferRetryDecorator transferRetryDecorator) {
        this.transferRetryDecorator = transferRetryDecorator;
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

    public TransferStrategyBuilder withNumTransferRetries(final int numRetries) {
        this.numTransferRetries = numRetries;
        return this;
    }

    public TransferStrategyBuilder withNumConcurrentTransferThreads(final int numConcurrentTransferThreads) {
        this.numConcurrentTransferThreads = numConcurrentTransferThreads;
        return this;
    }

    public TransferStrategyBuilder withNumChunkAttemptRetries(final int numChunkAttemptRetries) {
        this.numChunkAttemptRetries = numChunkAttemptRetries;
        return this;
    }

    public TransferStrategyBuilder withRetryDelayInSeconds(final int retryDelayInSeconds) {
        this.chunkRetryDelayInSeconds = retryDelayInSeconds;
        return this;
    }

    public TransferStrategyBuilder withDs3Client(final Ds3Client ds3Client) {
        this.ds3Client = ds3Client;
        return this;
    }

    public TransferStrategyBuilder withMasterObjectList(final MasterObjectList masterObjectList) {
        this.masterObjectList = masterObjectList;
        jobId = masterObjectList.getJobId().toString();
        bucketName = masterObjectList.getBucketName();
        return this;
    }

    public TransferStrategyBuilder withRangesForBlobs(final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs) {
        this.rangesForBlobs = rangesForBlobs;
        return this;
    }

    public TransferStrategyBuilder withMetadataAccess(final Ds3ClientHelpers.MetadataAccess metadataAccess) {
        this.metadataAccess = metadataAccess;
        return this;
    }

    public TransferStrategyBuilder withChunkAttemptRetryBehavior(final RetryBehavior chunkAttemptRetryBehavior) {
        this.chunkAttemptRetryBehavior = chunkAttemptRetryBehavior;
        return this;
    }

    public TransferStrategyBuilder withChunkAttemptRetryDelayBehavior(final ChunkAttemptRetryDelayBehavior chunkAttemptRetryDelayBehavior) {
        this.chunkAttemptRetryDelayBehavior = chunkAttemptRetryDelayBehavior;
        return this;
    }

    public TransferStrategyBuilder withEventRunner(final EventRunner eventRunner) {
        this.eventRunner = eventRunner;
        return this;
    }

    // TODO: implement these
    // public TransferStrategy makePutSequentialTransferStrategy() { }
    // public TransferStrategy makePutRandomTransferStrategy() { }

    public TransferStrategy makeOriginalSdkSemanticsPutTransferStrategy() {
        Preconditions.checkNotNull(channelBuilder, "channelBuilder my not be null");

        channelStrategy = new RandomAccessChannelStrategy(channelBuilder, rangesForBlobs, new NullChannelPreparable());

        getOrMakeTransferRetryDecorator();



        return makeTransferStrategy(
                new BlobStrategyMaker() {
                    @Override
                    public BlobStrategy makeBlobStrategy(final Ds3Client client,
                                                         final MasterObjectList masterObjectList,
                                                         final EventDispatcher eventDispatcher)
                    {
                        return new PutSequentialBlobStrategy(ds3Client,
                                masterObjectList,
                                eventDispatcher,
                                getOrMakeChunkAttemptRetryBehavior(),
                                getOrMakeChunkAllocationRetryDelayBehavior()
                                );
                    }
                },
                new TransferMethodMaker() {
                    @Override
                    public TransferMethod makeTransferMethod() {
                        return makePutTransferMethod();
                    }
                });
    }



    private JobPartTracker getOrMakeJobPartTrackerForPutJob(final List<Objects> chunksNotYetCompleted) {
        if (jobPartTracker != null) {
            return jobPartTracker;
        }

        final JobPartTracker result = JobPartTrackerFactory.buildPartTracker(Iterables.concat(getBlobs(chunksNotYetCompleted)), eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                eventDispatcher.emitObjectCompletedEvent(name);
            }
        });

        jobPartTracker = result;

        return jobPartTracker;
    }

    private ImmutableList<BulkObject> getBlobs(final List<Objects> chunks) {
        final ImmutableList.Builder<BulkObject> builder = ImmutableList.builder();
        for (final Objects objects : chunks) {
            builder.addAll(objects.getObjects());
        }
        return builder.build();
    }

    private TransferRetryDecorator getOrMakeTransferRetryDecorator() {
        if (transferRetryDecorator != null) {
            return transferRetryDecorator;
        }

        if (numTransferRetries > 0) {
            transferRetryDecorator = new MaxNumObjectTransferAttemptsDecorator(numTransferRetries);
        } else {
            transferRetryDecorator = new ContinueForeverTransferRetryDecorator();
        }

        return transferRetryDecorator;
    }

    private RetryBehavior getOrMakeChunkAttemptRetryBehavior() {
        if (chunkAttemptRetryBehavior != null) {
            return chunkAttemptRetryBehavior;
        }

        if (numChunkAttemptRetries > 0) {
            chunkAttemptRetryBehavior = new MaxChunkAttemptsRetryBehavior(numChunkAttemptRetries);
        } else {
            chunkAttemptRetryBehavior = new ContinueForeverChunkAttemptsRetryBehavior();
        }

        return chunkAttemptRetryBehavior;
    }

    private ChunkAttemptRetryDelayBehavior getOrMakeChunkAllocationRetryDelayBehavior() {
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");

        if (chunkAttemptRetryDelayBehavior != null) {
            return chunkAttemptRetryDelayBehavior;
        }

        if (chunkRetryDelayInSeconds > 0) {
            chunkAttemptRetryDelayBehavior = new ClientDefinedChunkAttemptRetryDelayBehavior(chunkRetryDelayInSeconds, eventDispatcher);
        } else {
            chunkAttemptRetryDelayBehavior = new BlackPearlChunkAttemptRetryDelayBehavior(eventDispatcher);
        }

        return chunkAttemptRetryDelayBehavior;
    }

    private TransferStrategy makeTransferStrategy(final BlobStrategyMaker blobStrategyMaker,
                                                  final TransferMethodMaker transferMethodMaker)
    {
        Preconditions.checkNotNull(ds3Client, "ds3Client may not be null.");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");
        Preconditions.checkNotNull(masterObjectList, "masterObjectList may not be null.");
        Preconditions.checkState( ! Guard.isStringNullOrEmpty(jobId), "jobId may not be null or an emptystring");
        Preconditions.checkNotNull(blobStrategyMaker, "blobStrategyMaker may not be null.");
        Preconditions.checkNotNull(transferMethodMaker, "transferMethodMaker may not be null.");
        Preconditions.checkNotNull(channelStrategy, "channelStrategy may not be null");
        Preconditions.checkNotNull(transferRetryDecorator, "transferRetryDecorator may not be null");

        Guard.throwOnNullOrEmptyString(bucketName, "bucketName may not be null or empty.");

        Guard.throwOnNullOrEmptyString(jobId, "jobId may not be null or empty.");

        blobStrategy = blobStrategyMaker.makeBlobStrategy(ds3Client,
                masterObjectList,
                eventDispatcher);

        eventDispatcher.attachBlobTransferredEventObserver(new BlobTransferredEventObserver(new UpdateStrategy<BulkObject>() {
            @Override
            public void update(final BulkObject eventData) {
                jobPartTracker.completePart(eventData.getName(), new ObjectPart(eventData.getOffset(), eventData.getLength()));
            }
        }));

        final TransferMethod transferMethod = transferMethodMaker.makeTransferMethod();

        final TransferStrategy transferStrategy;

        if (numConcurrentTransferThreads > 1) {
            transferStrategy = new MultiThreadedTransferStrategy(blobStrategy,
                    jobState,
                    numConcurrentTransferThreads)
                    .withTransferMethod(transferMethod);
        } else {
            transferStrategy = new SingleThreadedTransferStrategy(blobStrategy, jobState)
                    .withTransferMethod(transferMethod);
        }

        return transferStrategy;
    }

    private TransferMethod makePutTransferMethod() {
        getOrMakeJobStateForPutJob();

        if (checksumType != ChecksumType.Type.NONE) {
            maybeAddChecksumFunction();
        }

        final TransferMethod transferMethod = new PutJobTransferMethod(channelStrategy,
                bucketName, jobId, eventDispatcher, checksumFunction, checksumType, metadataAccess);

        if (transferRetryDecorator != null) {
            return transferRetryDecorator.wrap(transferMethod);
        }

        return transferMethod;
    }

    private JobState getOrMakeJobStateForPutJob() {
        if (jobState != null) {
            return jobState;
        }

        Preconditions.checkNotNull(masterObjectList, "masterObjectList may not be null.");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");
        Preconditions.checkNotNull(eventRunner, "eventRunner may not be null.");

        List<Objects> chunks = masterObjectList.getObjects();

        if (chunks == null) {
            chunks = new ArrayList<>();
        }

        final List<Objects> chunksNotYetCompleted = StrategyUtils.filterChunks(chunks);

        getOrMakeJobPartTrackerForPutJob(chunksNotYetCompleted);

        jobState = new JobState(chunksNotYetCompleted, jobPartTracker);

        return jobState;
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
                            .usingSystemWithEndpoint(ds3Client.getConnectionDetails().getEndpoint())
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
        Preconditions.checkNotNull(channelBuilder, "channelBuilder my not be null");

        channelStrategy = new RandomAccessChannelStrategy(channelBuilder, rangesForBlobs, new TruncatingChannelPreparable());

        getOrMakeTransferRetryDecorator();



        return makeTransferStrategy(
                new BlobStrategyMaker() {
                    @Override
                    public BlobStrategy makeBlobStrategy(final Ds3Client client,
                                                         final MasterObjectList masterObjectList,
                                                         final EventDispatcher eventDispatcher) {
                        return new GetSequentialBlobStrategy(ds3Client,
                                masterObjectList,
                                eventDispatcher,
                                getOrMakeChunkAttemptRetryBehavior(),
                                getOrMakeChunkAllocationRetryDelayBehavior());
                    }
                },
                new TransferMethodMaker() {
                    @Override
                    public TransferMethod makeTransferMethod() {
                        return makeGetTransferMethod();
                    }
                });
    }

    private JobPartTracker getOrMakeJobPartTrackerForGetJob(final List<Objects> chunks) {
        if (jobPartTracker != null) {
            return jobPartTracker;
        }

        final JobPartTracker result = JobPartTrackerFactory.buildPartTracker(getBlobs(chunks), eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                eventDispatcher.emitObjectCompletedEvent(name);
            }
        });

        jobPartTracker = result;

        return jobPartTracker;
    }

    private TransferMethod makeGetTransferMethod() {
        getOrMakeJobStateForGetJob();

        final TransferMethod transferMethod = new GetJobNetworkFailureRetryDecorator(channelStrategy,
                bucketName, jobId, eventDispatcher, rangesForBlobs);

        if (transferRetryDecorator != null) {
            return transferRetryDecorator.wrap(transferMethod);
        }

        return transferMethod;
    }

    private JobState getOrMakeJobStateForGetJob() {
        if (jobState != null) {
            return jobState;
        }

        Preconditions.checkNotNull(masterObjectList, "masterObjectList may not be null.");
        Preconditions.checkNotNull(eventDispatcher, "eventDispatcher may not be null.");
        Preconditions.checkNotNull(eventRunner, "eventRunner may not be null.");

        final List<Objects> chunks = masterObjectList.getObjects();

        getOrMakeJobPartTrackerForGetJob(chunks);

        jobState = new JobState(chunks, jobPartTracker);

        return jobState;
    }
}
