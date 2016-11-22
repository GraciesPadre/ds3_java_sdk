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
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.util.HashSet;
import java.util.Set;

public class EventDispatcherImpl implements EventDispatcher {
    private EventRunner eventRunner;

    private Set<FailureEventObserver> failureEventObservers;
    private Set<WaitingForChunksObserver> waitingForChunksObservers;
    private Set<ChecksumObserver> checksumObservers;

    private Set<DataTransferredObserver> dataTransferredObservers;
    private Set<ObjectCompletedObserver> objectCompletedObservers;

    private final EventDispatcherStrategy eventDispatcherStrategy;

    public EventDispatcherImpl(final EventRunner eventRunner, final JobPartTracker jobPartTracker) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker must not be null.");

        init(eventRunner);

        eventDispatcherStrategy = new EventDispatcherStrategy() {
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
            public void emitDataTransferredEvent(final BulkObject blob) {
                jobPartTracker.completePart(blob.getName(), new ObjectPart(blob.getOffset(), blob.getLength()));
            }

            @Override
            public void emitObjectCompletedEvent(final BulkObject blob) {
                emitDataTransferredEvent(blob);
            }
        };
    }

    private void init(final EventRunner eventRunner) {
        this.eventRunner = eventRunner;

        this.failureEventObservers = new HashSet<>();
        this.waitingForChunksObservers = new HashSet<>();
        this.checksumObservers = new HashSet<>();
    }

    public EventDispatcherImpl(final EventRunner eventRunner) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");

        this.eventRunner = eventRunner;

        init(eventRunner);

        dataTransferredObservers = new HashSet<>();
        objectCompletedObservers = new HashSet<>();

        eventDispatcherStrategy = new EventDispatcherStrategy() {
            @Override
            public void attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
                dataTransferredObservers.add(dataTransferredObserver);
            }

            @Override
            public void removeDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
                dataTransferredObservers.remove(dataTransferredObserver);
            }

            @Override
            public void attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
                objectCompletedObservers.add(objectCompletedObserver);
            }

            @Override
            public void removeObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
                objectCompletedObservers.remove(objectCompletedObserver);
            }

            @Override
            public void emitDataTransferredEvent(final BulkObject blob) {
                for (final DataTransferredObserver dataTransferredObserver : dataTransferredObservers) {
                    eventRunner.emitEvent(new Runnable() {
                        @Override
                        public void run() {
                            dataTransferredObserver.update(blob.getLength());
                        }
                    });
                }
            }

            @Override
            public void emitObjectCompletedEvent(final BulkObject blob) {
                for (final ObjectCompletedObserver objectCompletedObserver : objectCompletedObservers) {
                    eventRunner.emitEvent(new Runnable() {
                        @Override
                        public void run() {
                            objectCompletedObserver.update(blob.getName());
                        }
                    });
                }
            }
        };
    }

    @Override
    public void attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        eventDispatcherStrategy.attachDataTransferredObserver(dataTransferredObserver);
    }

    @Override
    public void removeDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        eventDispatcherStrategy.removeDataTransferredObserver(dataTransferredObserver);
    }

    @Override
    public void attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        eventDispatcherStrategy.attachObjectCompletedObserver(objectCompletedObserver);
    }

    @Override
    public void removeObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        eventDispatcherStrategy.removeObjectCompletedObserver(objectCompletedObserver);
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

    @Override
    public void emitDataTransferredEvent(final BulkObject blob) {
        eventDispatcherStrategy.emitDataTransferredEvent(blob);
    }

    @Override
    public void emitObjectCompletedEvent(final BulkObject blob) {
        eventDispatcherStrategy.emitObjectCompletedEvent(blob);
    }
}
