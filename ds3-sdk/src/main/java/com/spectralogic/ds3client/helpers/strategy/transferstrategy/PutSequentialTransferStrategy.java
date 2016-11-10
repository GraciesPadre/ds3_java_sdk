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

import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.io.IOException;

public class PutSequentialTransferStrategy implements TransferStrategy {
    private final ChannelStrategy channelStrategy;
    private final BlobStrategy blobStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventRegistrar eventRegistrar;

    private DataTransceiver dataTransceiver;

    public PutSequentialTransferStrategy(final ChannelStrategy channelStrategy,
                                         final BlobStrategy blobStrategy,
                                         final String bucketName,
                                         final String jobId,
                                         final EventRegistrar eventRegistrar)
    {
        this.channelStrategy = channelStrategy;
        this.blobStrategy = blobStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventRegistrar = eventRegistrar;
    }

    public PutSequentialTransferStrategy withDataTransceiver(final DataTransceiver dataTranceiver) {
        this.dataTransceiver = dataTranceiver;
        return this;
    }

    @Override
    public void transfer() throws IOException, InterruptedException {
        final Iterable<JobPart> workQueue = blobStrategy.getWork();

        for (final JobPart jobPart : workQueue) {
            dataTransceiver.transferJobPart(jobPart);
        }
    }

    @Override
    public void attachChecksumListener(final ChecksumListener listener) {
        eventRegistrar.attachChecksumListener(listener);
    }

    @Override
    public void removeChecksumListener(final ChecksumListener listener) {
        eventRegistrar.removeChecksumListener(listener);
    }

    @Override
    public void attachDataTransferredListener(final DataTransferredListener listener) {
        eventRegistrar.attachDataTransferredListener(listener);
    }

    @Override
    public void removeDataTransferredListener(final DataTransferredListener listener) {
        eventRegistrar.removeDataTransferredListener(listener);
    }

    @Override
    public void attachObjectCompletedListener(final ObjectCompletedListener listener) {
        eventRegistrar.attachObjectCompletedListener(listener);
    }

    @Override
    public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
        eventRegistrar.removeObjectCompletedListener(listener);
    }

    @Override
    public void attachWaitingForChunksListener(final WaitingForChunksListener listener) {
        eventRegistrar.attachWaitingForChunksListener(listener);
    }

    @Override
    public void removeWaitingForChunksListener(final WaitingForChunksListener listener) {
        eventRegistrar.removeWaitingForChunksListener(listener);
    }

    @Override
    public void attachFailureEventListener(final FailureEventListener listener) {
        eventRegistrar.attachFailureEventListener(listener);
    }

    @Override
    public void removeFailureEventListener(final FailureEventListener listener) {
        eventRegistrar.removeFailureEventListener(listener);
    }

    @Override
    public void emitChecksumEvent(final BulkObject blob, final ChecksumType.Type checksumType, final String checksum) {
        eventRegistrar.emitChecksumEvent(blob, checksumType, checksum);
    }

    @Override
    public void emitFailureEvent(final FailureEvent failureEvent) {
        eventRegistrar.emitFailureEvent(failureEvent);
    }

    @Override
    public void emitWaitingForChunksEvents(final int secondsToDelay) {
        eventRegistrar.emitWaitingForChunksEvents(secondsToDelay);
    }
}
