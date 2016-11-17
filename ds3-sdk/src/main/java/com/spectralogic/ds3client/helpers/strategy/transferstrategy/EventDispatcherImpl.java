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
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.util.HashSet;
import java.util.Set;

public class EventDispatcherImpl implements EventDispatcher {
    private final EventRunner eventRunner;
    private final JobPartTracker jobPartTracker;
    private final Set<FailureEventObserver> failureEventObservers;
    private final Set<WaitingForChunksObserver> waitingForChunksObservers;
    private final Set<ChecksumObserver> checksumObservers;

    public EventDispatcherImpl(final EventRunner eventRunner, final JobPartTracker jobPartTracker) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker must not be null.");

        this.eventRunner = eventRunner;
        this.jobPartTracker = jobPartTracker;

        this.failureEventObservers = new HashSet<>();
        this.waitingForChunksObservers = new HashSet<>();
        this.checksumObservers = new HashSet<>();
    }

    @Override
    public void attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        jobPartTracker.attachDataTransferredListener(dataTransferredObserver.getDataTransferredListener());
    }

    @Override
    public void removeDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        jobPartTracker.removeDataTransferredListener(dataTransferredObserver.getDataTransferredListener());
    }

    @Override
    public void attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        jobPartTracker.attachObjectCompletedListener(objectCompletedObserver.getObjectCompletedListener());
    }

    @Override
    public void removeObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        jobPartTracker.removeObjectCompletedListener(objectCompletedObserver.getObjectCompletedListener());
    }

    @Override
    public void attachChecksumObserver(final ChecksumObserver checksumObserver) {
        checksumObservers.add(checksumObserver);
    }

    @Override
    public void removeChecksumObserver(final ChecksumObserver checksumObserver) {
        checksumObservers.remove(checksumObserver);
    }

    @Override
    public void attachWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver) {
        waitingForChunksObservers.add(waitingForChunksObserver);
    }

    @Override
    public void removeWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver) {
        waitingForChunksObservers.remove(waitingForChunksObserver);
    }

    @Override
    public void attachFailureEventObserver(final FailureEventObserver failureEventObserver) {
        failureEventObservers.add(failureEventObserver);
    }

    @Override
    public void removeFailureEventObserver(final FailureEventObserver failureEventObserver) {
        failureEventObservers.remove(failureEventObserver);
    }

    @Override
    public void emitFailureEvent(final FailureEvent failureEvent) {
        for (final FailureEventObserver failureEventObserver : failureEventObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    failureEventObserver.update(failureEvent);
                }
            });
        }
    }

    @Override
    public void emitWaitingForChunksEvents(final int secondsToDelay) {
        for (final WaitingForChunksObserver waitingForChunksObserver : waitingForChunksObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    waitingForChunksObserver.update(secondsToDelay);
                }
            });
        }
    }

    @Override
    public void emitChecksumEvent(final BulkObject bulkObject, final ChecksumType.Type type, final String checksum) {
        for (final ChecksumObserver observer : checksumObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    observer.update(new ChecksumEvent(bulkObject, type, checksum));
                }
            });
        }
    }
}
