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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.MetadataReceivedListener;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.events.MetadataEvent;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.networking.Metadata;

import java.util.Set;
import java.util.UUID;

public class EventDispatcherImpl implements EventDispatcher {
    private final EventRunner eventRunner;

    private final BiMap<String, DataTransferredObserver> dataTransferredObservers = HashBiMap.create();
    private final BiMap<String, ObjectCompletedObserver> objectCompletedObservers = HashBiMap.create();
    private final BiMap<String, ChecksumObserver> checksumObservers = HashBiMap.create();
    private final BiMap<String, WaitingForChunksObserver> waitingForChunksObservers = HashBiMap.create();
    private final BiMap<String, FailureEventObserver> failureEventObservers = HashBiMap.create();
    private final BiMap<String, MetaDataReceivedObserver> metaDataReceivedObservers = HashBiMap.create();
    private final BiMap<String, BlobTransferredEventObserver> blobTransferredEventObservers = HashBiMap.create();

    private final Set<DataTransferredListener> dataTransferredListeners = Sets.newIdentityHashSet();
    private final Set<ObjectCompletedListener> objectCompletedListeners = Sets.newIdentityHashSet();
    private final Set<MetadataReceivedListener> metadataReceivedListeners = Sets.newIdentityHashSet();
    private final Set<ChecksumListener> checksumListeners = Sets.newIdentityHashSet();
    private final Set<WaitingForChunksListener> waitingForChunksListeners = Sets.newIdentityHashSet();
    private final Set<FailureEventListener> failureEventListeners = Sets.newIdentityHashSet();

    public EventDispatcherImpl(final EventRunner eventRunner) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");
        this.eventRunner = eventRunner;
    }

    @Override
    public String attachDataTransferredObserver(final DataTransferredObserver dataTransferredObserver) {
        return registerForEvents(dataTransferredObservers, dataTransferredObserver);
    }

    private String registerForEvents(final BiMap eventObservers, final Observer observer) {
        String observerId = (String)eventObservers.inverse().get(observer);

        if (observerId == null) {
            observerId = makeNewObserverId();
            eventObservers.put(observerId, observer);
        }

        return observerId;
    }

    private String makeNewObserverId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void removeDataTransferredObserver(final String dataTransferredObserverId) {
        dataTransferredObservers.remove(dataTransferredObserverId);
    }

    @Override
    public String attachObjectCompletedObserver(final ObjectCompletedObserver objectCompletedObserver) {
        return registerForEvents(objectCompletedObservers, objectCompletedObserver);
    }

    @Override
    public void removeObjectCompletedObserver(final String objectCompletedObserverId) {
        objectCompletedObservers.remove(objectCompletedObserverId);
    }

    @Override
    public String attachChecksumObserver(final ChecksumObserver checksumObserver) {
        return registerForEvents(checksumObservers, checksumObserver);
    }

    @Override
    public void removeChecksumObserver(final String checksumObserverId) {
        checksumObservers.remove(checksumObserverId);
    }

    @Override
    public String attachWaitingForChunksObserver(final WaitingForChunksObserver waitingForChunksObserver) {
        return registerForEvents(waitingForChunksObservers, waitingForChunksObserver);
    }

    @Override
    public void removeWaitingForChunksObserver(final String waitingForChunksObserverId) {
        waitingForChunksObservers.remove(waitingForChunksObserverId);
    }

    @Override
    public String attachFailureEventObserver(final FailureEventObserver failureEventObserver) {
        return registerForEvents(failureEventObservers, failureEventObserver);
    }

    @Override
    public void removeFailureEventObserver(final String failureEventObserverId) {
        failureEventObservers.remove(failureEventObserverId);
    }

    @Override
    public String attachMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver) {
        return registerForEvents(metaDataReceivedObservers, metaDataReceivedObserver);
    }

    @Override
    public void removeMetadataReceivedEventObserver(final String metaDataReceivedObserverId) {
        metaDataReceivedObservers.remove(metaDataReceivedObserverId);
    }

    @Override
    public String attachBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver) {
        return registerForEvents(blobTransferredEventObservers, blobTransferredEventObserver);
    }

    @Override
    public void removeBlobTransferredEventObserver(final String blobTransferredEventObserverId) {
        blobTransferredEventObservers.remove(blobTransferredEventObserverId);
    }

    @Override
    public void emitFailureEvent(final FailureEvent failureEvent) {
        emitEvents(failureEventObservers, failureEvent);
    }

    private <T> void emitEvents(final BiMap eventObservers, final T eventData) {
        for (final Object eventObserver : eventObservers.values()) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    ((Observer<T>)eventObserver).update(eventData);
                }
            });
        }
    }

    @Override
    public void emitWaitingForChunksEvents(final int secondsToDelay) {
        emitEvents(waitingForChunksObservers, secondsToDelay);
    }

    @Override
    public void emitChecksumEvent(final BulkObject blob, final ChecksumType.Type type, final String checksum) {
        emitEvents(checksumObservers, new ChecksumEvent(blob, type, checksum));
    }

    @Override
    public void emitDataTransferredEvent(final BulkObject blob) {
        emitEvents(dataTransferredObservers, blob);
    }

    @Override
    public void emitObjectCompletedEvent(final BulkObject blob) {
        emitEvents(objectCompletedObservers, blob);
    }

    @Override
    public void emitMetaDataReceivedEvent(final String objectName, final Metadata metadata) {
        emitEvents(metaDataReceivedObservers, metadata);
    }

    @Override
    public void emitBlobTransferredEvent(final BulkObject blob) {
        emitEvents(blobTransferredEventObservers, blob);
    }

    /*
    private final Set<FailureEventObserver> failureEventObservers = Sets.newIdentityHashSet();
    private final Set<WaitingForChunksObserver> waitingForChunksObservers = Sets.newIdentityHashSet();
    private final Set<ChecksumObserver> checksumObservers = Sets.newIdentityHashSet();
    private final Set<MetaDataReceivedObserver> metaDataReceivedObservers = Sets.newIdentityHashSet();

    private Set<DataTransferredObserver> dataTransferredObservers;
    private Set<ObjectCompletedObserver> objectCompletedObservers;

    private final Set<BlobTransferredEventObserver> blobTransferredEventObservers = Sets.newIdentityHashSet();

    private final EventDispatcherStrategy eventDispatcherStrategy;

    public EventDispatcherImpl(final EventRunner eventRunner, final JobPartTracker jobPartTracker) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");
        Preconditions.checkNotNull(jobPartTracker, "jobPartTracker must not be null.");

        this.eventRunner = eventRunner;

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

    public EventDispatcherImpl(final EventRunner eventRunner) {
        Preconditions.checkNotNull(eventRunner, "eventRunner must not be null.");

        this.eventRunner = eventRunner;

        dataTransferredObservers = Sets.newIdentityHashSet();
        objectCompletedObservers = Sets.newIdentityHashSet();

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
    public void attachMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver) {
        metaDataReceivedObservers.add(metaDataReceivedObserver);
    }

    @Override
    public void removeMetadataReceivedEventObserver(final MetaDataReceivedObserver metaDataReceivedObserver) {
        metaDataReceivedObservers.remove(metaDataReceivedObserver);
    }

    @Override
    public void attachBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver) {
        blobTransferredEventObservers.add(blobTransferredEventObserver);
    }

    @Override
    public void removeBlobTransferredEventObserver(final BlobTransferredEventObserver blobTransferredEventObserver) {
        blobTransferredEventObservers.remove(blobTransferredEventObserver);
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
        for (final ChecksumObserver checksumObserver : checksumObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    checksumObserver.update(new ChecksumEvent(bulkObject, type, checksum));
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

    @Override
    public void emitMetaDataReceivedEvent(final String objectName, final Metadata metadata) {
        for (final MetaDataReceivedObserver metaDataReceivedObserver : metaDataReceivedObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    metaDataReceivedObserver.update(new MetadataEvent(objectName, metadata));
                }
            });
        }
    }

    @Override
    public void emitBlobTransferredEvent(final BulkObject blob) {
        for (final BlobTransferredEventObserver blobTransferredEventObserver : blobTransferredEventObservers) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    blobTransferredEventObserver.update(blob);
                }
            });
        }
    }
    */
}
