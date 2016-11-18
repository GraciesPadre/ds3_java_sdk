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
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.utils.Guard;

public final class TransferStrategyBuilder {
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

    public TransferStrategyBuilder withEventRegistrar(final EventDispatcher eventDispatcher) {
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

        final PutSequentialTransferStrategy putSequentialTransferStrategy = new PutSequentialTransferStrategy(
                channelStrategy, blobStrategy, bucketName, jobId, eventDispatcher);

        return putSequentialTransferStrategy.withDataTransceiver(makeDataTransceiver(putSequentialTransferStrategy));
    }

    private TransferMethod makeDataTransceiver(final TransferStrategy transferStrategy) {
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker may not be null.");

        final TransferMethod transferMethod = new PutJobTransferMethod(transferStrategy, channelStrategy,
                blobStrategy, bucketName, jobId, jobPartTracker, checksumFunction, checksumType);

        if (transferRetryBehavior != null) {
            return transferRetryBehavior.wrap(transferMethod);
        }

        return transferMethod;
    }
}
