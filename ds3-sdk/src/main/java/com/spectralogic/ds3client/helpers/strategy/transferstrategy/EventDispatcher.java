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
import com.spectralogic.ds3client.helpers.MetadataReceivedListener;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.networking.Metadata;

public interface EventDispatcher {
    /**
     * Attaches an event handler that is invoked when a blob is successfully
     * transferred to Spectra S3.
     */
    String attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver);
    void removeDataTransferredObserver(final String dataTransferredObserverId);

    /**
     * Attaches an event handler that is invoked when a full object is
     * successfully transferred to Spectra S3.
     */
    String attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver);
    void removeObjectCompletedObserver(final String objectCompletedObserverId);

    /**
     * Attaches an event handler that is invoked when an object checksum is received.
     */
    String attachChecksumObserver(final ChecksumObserver checksumObserver);
    void removeChecksumObserver(final String checksumObserverId);

    /**
     * Attaches an event handler that will be invoked only when there are no chunks available
     * for processing.
     */
    String attachWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver);
    void removeWaitingForChunksObserver(final String waitingForChunksObserverId);

    /**
     * Attaches an event handler when an object transfer fails
     */
    String attachFailureEventObserver(final FailureEventObserver failureEventObserver);
    void removeFailureEventObserver(final String failureEventObserverId);

    String attachMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver);
    void removeMetadataReceivedEventObserver(final String metaDataReceivedObserverId);

    String attachBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver);
    void removeBlobTransferredEventObserver(final String blobTransferredEventObserverId);

    void emitFailureEvent(final FailureEvent failureEvent);
    void emitWaitingForChunksEvents(final int secondsToDelay);
    void emitChecksumEvent(final BulkObject blob, final ChecksumType.Type type, final String checksum);
    void emitDataTransferredEvent(final BulkObject blob);
    void emitObjectCompletedEvent(final BulkObject blob);
    void emitMetaDataReceivedEvent(final String objectName, final Metadata metadata);
    void emitBlobTransferredEvent(final BulkObject blob);
}
