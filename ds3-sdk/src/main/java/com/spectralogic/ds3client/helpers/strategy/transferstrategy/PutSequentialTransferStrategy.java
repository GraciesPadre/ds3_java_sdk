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

import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.networking.Metadata;

import java.io.IOException;

public class PutSequentialTransferStrategy implements TransferStrategy {
    private final ChannelStrategy channelStrategy;
    private final BlobStrategy blobStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventDispatcher eventDispatcher;

    private TransferMethod transferMethod;

    public PutSequentialTransferStrategy(final ChannelStrategy channelStrategy,
                                         final BlobStrategy blobStrategy,
                                         final String bucketName,
                                         final String jobId,
                                         final EventDispatcher eventDispatcher)
    {
        this.channelStrategy = channelStrategy;
        this.blobStrategy = blobStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventDispatcher = eventDispatcher;
    }

    public PutSequentialTransferStrategy withTransferMethod(final TransferMethod transferMethod) {
        this.transferMethod = transferMethod;
        return this;
    }

    @Override
    public void transfer() throws IOException, InterruptedException {
        final Iterable<JobPart> workQueue = blobStrategy.getWork();

        for (final JobPart jobPart : workQueue) {
            transferMethod.transferJobPart(jobPart);
        }
    }

    /*
    @Override
    public void attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
    }

    @Override
    public void removeDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        eventDispatcher.removeDataTransferredObserver(dataTransferredObserver);
    }

    @Override
    public void attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
    }

    @Override
    public void removeObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        eventDispatcher.removeObjectCompletedObserver(objectCompletedObserver);
    }

    @Override
    public void attachChecksumObserver(final ChecksumObserver checksumObserver) {
        eventDispatcher.attachChecksumObserver(checksumObserver);
    }

    @Override
    public void removeChecksumObserver(final ChecksumObserver checksumObserver) {
        eventDispatcher.removeChecksumObserver(checksumObserver);
    }

    @Override
    public void attachWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver) {
        eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);
    }

    @Override
    public void removeWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver) {
        eventDispatcher.removeWaitingForChunksObserver(waitingForChunksObserver);
    }

    @Override
    public void attachFailureEventObserver(final FailureEventObserver failureEventObserver) {
        eventDispatcher.attachFailureEventObserver(failureEventObserver);
    }

    @Override
    public void removeFailureEventObserver(final FailureEventObserver failureEventObserver) {
        eventDispatcher.removeFailureEventObserver(failureEventObserver);
    }

    @Override
    public void attachMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver) {
        eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);
    }

    @Override
    public void removeMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver) {
        eventDispatcher.removeMetadataReceivedEventObserver(metaDataReceivedObserver);
    }

    @Override
    public void attachBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver) {
        eventDispatcher.attachBlobTransferredEventObserver(blobTransferredEventObserver);
    }

    @Override
    public void removeBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver) {
        eventDispatcher.removeBlobTransferredEventObserver(blobTransferredEventObserver);
    }

    @Override
    public void emitChecksumEvent(final BulkObject blob, final ChecksumType.Type checksumType, final String checksum) {
        eventDispatcher.emitChecksumEvent(blob, checksumType, checksum);
    }

    @Override
    public void emitFailureEvent(final FailureEvent failureEvent) {
        eventDispatcher.emitFailureEvent(failureEvent);
    }

    @Override
    public void emitWaitingForChunksEvents(final int secondsToDelay) {
        eventDispatcher.emitWaitingForChunksEvents(secondsToDelay);
    }

    @Override
    public void emitDataTransferredEvent(final BulkObject blob) {
        eventDispatcher.emitDataTransferredEvent(blob);
    }

    @Override
    public void emitObjectCompletedEvent(final BulkObject blob) {
        eventDispatcher.emitObjectCompletedEvent(blob);
    }

    @Override
    public void emitMetaDataReceivedEvent(final String objectName, final Metadata metadata) {
        eventDispatcher.emitMetaDataReceivedEvent(objectName, metadata);
    }

    @Override
    public void emitBlobTransferredEvent(final BulkObject blob) {
        eventDispatcher.emitBlobTransferredEvent(blob);
    }
    */
}
