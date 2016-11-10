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
import com.google.common.collect.Sets;
import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.util.Set;

public class EventRegistrarImpl implements EventRegistrar {
    private final EventRunner eventRunner;
    private final JobPartTracker jobPartTracker;
    private final Set<FailureEventListener> failureEventListeners;
    private final Set<WaitingForChunksListener> waitingForChunksListeners;
    private final Set<ChecksumListener> checksumListeners;

    public EventRegistrarImpl(final EventRunner eventRunner, final JobPartTracker jobPartTracker) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker must not be null.");

        this.eventRunner = eventRunner;
        this.jobPartTracker = jobPartTracker;

        this.failureEventListeners = Sets.newIdentityHashSet();
        this.waitingForChunksListeners = Sets.newIdentityHashSet();
        this.checksumListeners = Sets.newIdentityHashSet();
    }

    @Override
    public void attachDataTransferredListener(final DataTransferredListener listener) {
        jobPartTracker.attachDataTransferredListener(listener);
    }

    @Override
    public void removeDataTransferredListener(final DataTransferredListener listener) {
        jobPartTracker.removeDataTransferredListener(listener);
    }

    @Override
    public void attachObjectCompletedListener(final ObjectCompletedListener listener) {
        jobPartTracker.attachObjectCompletedListener(listener);
    }

    @Override
    public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
        jobPartTracker.removeObjectCompletedListener(listener);
    }

    @Override
    public void attachChecksumListener(final ChecksumListener listener) {
        checksumListeners.add(listener);
    }

    @Override
    public void removeChecksumListener(final ChecksumListener listener) {
        checksumListeners.remove(listener);
    }

    @Override
    public void attachWaitingForChunksListener(final WaitingForChunksListener listener) {
        waitingForChunksListeners.add(listener);
    }

    @Override
    public void removeWaitingForChunksListener(final WaitingForChunksListener listener) {
        waitingForChunksListeners.remove(listener);
    }

    @Override
    public void attachFailureEventListener(final FailureEventListener listener) {
        failureEventListeners.add(listener);
    }

    @Override
    public void removeFailureEventListener(final FailureEventListener listener) {
        failureEventListeners.remove(listener);
    }

    @Override
    public void emitFailureEvent(final FailureEvent failureEvent) {
        for (final FailureEventListener failureEventListener : failureEventListeners) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    failureEventListener.onFailure(failureEvent);
                }
            });
        }
    }

    @Override
    public void emitWaitingForChunksEvents(final int secondsToDelay) {
        for (final WaitingForChunksListener waitingForChunksListener : waitingForChunksListeners) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    waitingForChunksListener.waiting(secondsToDelay);
                }
            });
        }
    }

    @Override
    public void emitChecksumEvent(final BulkObject bulkObject, final ChecksumType.Type type, final String checksum) {
        for (final ChecksumListener listener : checksumListeners) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    listener.value(bulkObject, type, checksum);
                }
            });
        }
    }
}
