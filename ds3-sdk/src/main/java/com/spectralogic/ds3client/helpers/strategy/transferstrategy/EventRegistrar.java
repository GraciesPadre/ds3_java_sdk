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
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

public interface EventRegistrar {
    /**
     * Attaches an event handler that is invoked when a blob is successfully
     * transferred to Spectra S3.
     */
    void attachDataTransferredListener(final DataTransferredListener listener);
    void removeDataTransferredListener(final DataTransferredListener listener);

    /**
     * Attaches an event handler that is invoked when a full object is
     * successfully transferred to Spectra S3.
     */
    void attachObjectCompletedListener(final ObjectCompletedListener listener);
    void removeObjectCompletedListener(final ObjectCompletedListener listener);

    /**
     * Attaches an event handler that is invoked when an object checksum is received.
     */
    void attachChecksumListener(final ChecksumListener listener);
    void removeChecksumListener(final ChecksumListener listener);

    /**
     * Attaches an event handler that will be invoked only when there are no chunks available
     * for processing.
     */
    void attachWaitingForChunksListener(final WaitingForChunksListener listener);
    void removeWaitingForChunksListener(final WaitingForChunksListener listener);

    /**
     * Attaches an event handler when an object transfer fails
     */
    void attachFailureEventListener(final FailureEventListener listener);
    void removeFailureEventListener(final FailureEventListener listener);

    void emitFailureEvent(final FailureEvent failureEvent);
    void emitWaitingForChunksEvents(final int secondsToDelay);
    void emitChecksumEvent(final BulkObject bulkObject, final ChecksumType.Type type, final String checksum);
}
