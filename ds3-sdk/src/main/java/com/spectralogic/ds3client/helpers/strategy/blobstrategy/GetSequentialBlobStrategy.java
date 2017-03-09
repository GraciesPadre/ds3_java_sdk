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

package com.spectralogic.ds3client.helpers.strategy.blobstrategy;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.GetJobChunksReadyForClientProcessingSpectraS3Request;
import com.spectralogic.ds3client.commands.spectrads3.GetJobChunksReadyForClientProcessingSpectraS3Response;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.strategy.StrategyUtils;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.JobNode;
import com.spectralogic.ds3client.models.MasterObjectList;
import com.spectralogic.ds3client.models.Objects;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetSequentialBlobStrategy extends AbstractBlobStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GetSequentialBlobStrategy.class);



    private final Set<UUID> processedChunks;

    private final Object lock = new Object();
    private final Set<String> activeBlobs = new HashSet<>();
    private ImmutableList<JobPart> outstandingJobParts;

    public GetSequentialBlobStrategy(final Ds3Client client,
                                     final MasterObjectList masterObjectList,
                                     final EventDispatcher eventDispatcher,
                                     final RetryBehavior retryBehavior,
                                     final ChunkAttemptRetryDelayBehavior chunkAttemptRetryDelayBehavior)
    {
        super(client, masterObjectList, eventDispatcher, retryBehavior, chunkAttemptRetryDelayBehavior);
        this.processedChunks = new HashSet<>();
    }

    @Override
    public Iterable<JobPart> getWork() throws IOException, InterruptedException {

        final MasterObjectList available = getAvailable();

        if (available == null || available.getObjects() == null) {
            final String jobId = available == null ? "" : (available.getJobId() == null ? "" : available.getJobId().toString());
            LOG.info("There is nothing to transfer for job " + jobId);
        }

        return getJobPartsForBlobsNotYetProcessed(available);
    }

    private MasterObjectList getAvailable() throws IOException, InterruptedException {
        do {
            final GetJobChunksReadyForClientProcessingSpectraS3Response availableJobChunks =
                    client().getJobChunksReadyForClientProcessingSpectraS3(new GetJobChunksReadyForClientProcessingSpectraS3Request(masterObjectList().getJobId().toString()));

            switch (availableJobChunks.getStatus()) {
                case AVAILABLE: {
                    retryBehavior().reset();
                    return availableJobChunks.getMasterObjectListResult();
                }
                case RETRYLATER: {
                    retryBehavior().invoke();

                    chunkAttemptRetryDelayBehavior().delay(availableJobChunks.getRetryAfterSeconds());

                    continue;
                }
                default:
                    assert false : "This line of code should be impossible to hit.";
            }
        } while(true);
    }

    private Iterable<JobPart> getJobPartsForBlobsNotYetProcessed(final MasterObjectList available) {
        final ImmutableMap<UUID, JobNode> jobNodes = StrategyUtils.buildNodeMap(available.getNodes());

        // filter any chunks that have been processed
        final FluentIterable<Objects> chunks = FluentIterable.from(available.getObjects());

        final ImmutableList.Builder<JobPart> filteredPartsBuilder = ImmutableList.builder();

        final FluentIterable<JobPart> jobParts = chunks.filter(new Predicate<Objects>() {
            @Override
            public boolean apply(@Nullable final Objects input) {
                return !processedChunks.contains(input.getChunkId());
            }
        }).transformAndConcat(new Function<Objects, Iterable<JobPart>>() {
            @Nullable
            @Override
            public Iterable<JobPart> apply(@Nullable final Objects objects) {
                return FluentIterable.from(objects.getObjects()).transform(new Function<BulkObject, JobPart>() {
                    @Nullable
                    @Override
                    public JobPart apply(@Nullable final BulkObject blob) {
                        return new JobPart(
                                StrategyUtils.getClient(jobNodes, objects.getNodeId(), client()),
                                blob);
                    }
                });
            }
        });

        final FluentIterable<JobPart> nextWorkParts = FluentIterable.from(
                getNextIterable(jobParts)
        ).filter(new Predicate<JobPart>() {
            @Override
            public boolean apply(@Nullable final JobPart input) {
                final String blobName = input.getBulkObject().getName();
                synchronized (lock) {
                    if (activeBlobs.contains(blobName)) {
                        filteredPartsBuilder.add(input);
                        return false;
                    } else {
                        activeBlobs.add(blobName);
                    }
                }
                return true;
            }
        });

        outstandingJobParts = filteredPartsBuilder.build();

        return nextWorkParts;
    }

    private Iterable<JobPart> getNextIterable(final FluentIterable<JobPart> jobParts) {
        if (outstandingJobParts == null) {
            return jobParts;
        }
        return Iterables.concat(outstandingJobParts, jobParts);
    }

    @Override
    public void blobCompleted(final BulkObject bulkObject) {
        synchronized (lock) {
            activeBlobs.remove(bulkObject.getName());
        }
    }
}
