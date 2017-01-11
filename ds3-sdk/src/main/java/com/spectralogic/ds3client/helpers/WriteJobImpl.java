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
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.helpers.ChunkTransferrer.ItemTransferrer;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.PutSequentialStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.OriginalChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.SequentialAggregatingChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.utils.Guard;
import com.spectralogic.ds3client.utils.SeekableByteChannelInputStream;
import com.spectralogic.ds3client.utils.hashing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
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

            final ChannelStrategy channelStrategy = new SequentialAggregatingChannelStrategy(new OriginalChannelStrategy(channelBuilder));

            final TransferStrategyBuilder transferStrategyBuilder = new TransferStrategyBuilder()
                    .withBlobStrategy(blobStrategy)
                    .withChannelStrategy(channelStrategy)
                    .withBucketName(masterObjectList.getBucketName())
                    .withJobId(getJobId().toString())
                    // .withTransferRetryBehavior(new MaxNumObjectTransferAttemptsBehavior(getObjectTransferAttempts()))
                    .withJobPartTracker(getJobPartTracker())
                    .withEventDispatcher(getEventDispatcher())
                    .withChecksumType(checksumType);

            if (checksumType != ChecksumType.Type.NONE) {
                if (checksumFunction == null) {
                    transferStrategyBuilder.withChecksumFunction(new ChecksumFunction() {
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
                                // TODO Add a filure event for this
                                LOG.error("Error computing checksum.", e);
                            }

                            return checksum;
                        }
                    });
                } else {
                    transferStrategyBuilder.withChecksumFunction(checksumFunction);
                }
            }

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

    private final class PutObjectTransferrerRetryDecorator implements ItemTransferrer {
        private final PutObjectTransferrer putObjectTransferrer;

        private PutObjectTransferrerRetryDecorator(final JobState jobState) {
            putObjectTransferrer = new PutObjectTransferrer(jobState);
        }

        @Override
        public void transferItem(final JobPart jobPart, final BulkObject ds3Object) throws IOException {
            WriteJobImpl.this.transferItem(jobPart, client, ds3Object, putObjectTransferrer);
        }
    }

    private final class PutObjectTransferrer implements ItemTransferrer {
        private final JobState jobState;

        private PutObjectTransferrer(final JobState jobState) {
            this.jobState = jobState;
        }

        @Override
        public void transferItem(final JobPart jobPart, final BulkObject ds3Object)
                throws IOException {
            client.putObject(createRequest(jobPart, ds3Object));
        }

        private PutObjectRequest createRequest(final JobPart jobPart, final BulkObject ds3Object) throws IOException {
            final PutObjectRequest request = new PutObjectRequest(
                    WriteJobImpl.this.masterObjectList.getBucketName(),
                    ds3Object.getName(),
                    this.jobState.getChannel(ds3Object.getName(), ds3Object.getOffset(), ds3Object.getLength()),
                    WriteJobImpl.this.getJobId().toString(),
                    ds3Object.getOffset(),
                    ds3Object.getLength()
            );

            if (ds3Object.getOffset() == 0 && metadataAccess != null) {
                final Map<String, String> metadata = metadataAccess.getMetadataValue(ds3Object.getName());
                if (Guard.isMapNullOrEmpty(metadata)) return request;
                final ImmutableMap<String, String> immutableMetadata = ImmutableMap.copyOf(metadata);
                for (final Map.Entry<String, String> value : immutableMetadata.entrySet()) {
                    request.withMetaData(value.getKey(), value.getValue());
                }
            }

            final String checksum = calculateChecksum(ds3Object,
                    this.jobState.getChannel(ds3Object.getName(), ds3Object.getOffset(), ds3Object.getLength()));

            if (checksum != null) {
                request.withChecksum(ChecksumType.value(checksum), WriteJobImpl.this.checksumType);
                emitChecksumEvents(ds3Object, WriteJobImpl.this.checksumType, checksum);
            }

            return request;
        }

        private String calculateChecksum(final BulkObject ds3Object, final ByteChannel channel) throws IOException {
            if (WriteJobImpl.this.checksumType != ChecksumType.Type.NONE) {
                if (WriteJobImpl.this.checksumFunction == null) {
                    LOG.info("Calculating {} checksum for blob: {}", WriteJobImpl.this.checksumType.toString(), ds3Object.toString());
                    final InputStream dataStream = Channels.newInputStream(channel);
                    final Hasher hasher = getHasher(WriteJobImpl.this.checksumType);
                    final String checksum = hashInputStream(hasher, dataStream);
                    LOG.info("Computed checksum for blob: {}", checksum);
                    return checksum;
                } else {
                    LOG.info("Getting checksum from user supplied callback for blob: {}", ds3Object.toString());
                    final String checksum = WriteJobImpl.this.checksumFunction.compute(ds3Object, channel);
                    LOG.info("User supplied checksum is: {}", checksum);
                    return checksum;
                }
            }
            return null;
        }

        private static final int READ_BUFFER_SIZE = 10 * 1024 * 1024;

        private String hashInputStream(final Hasher digest, final InputStream stream) throws IOException {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];
            int bytesRead;

            while (true) {
                bytesRead = stream.read(buffer);

                if (bytesRead < 0) {
                    break;
                }

                digest.update(buffer, 0, bytesRead);
            }

            return digest.digest();
        }

        private Hasher getHasher(final ChecksumType.Type checksumType) {
            switch (checksumType) {
                case MD5:
                    return new MD5Hasher();
                case SHA_256:
                    return new SHA256Hasher();
                case SHA_512:
                    return new SHA512Hasher();
                case CRC_32:
                    return new CRC32Hasher();
                case CRC_32C:
                    return new CRC32CHasher();
                default:
                    throw new RuntimeException("Unknown checksum type " + checksumType.toString());
            }
        }
    }
}

