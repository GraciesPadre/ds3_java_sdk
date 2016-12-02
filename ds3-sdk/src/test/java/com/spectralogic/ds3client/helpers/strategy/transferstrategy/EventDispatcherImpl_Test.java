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

import com.spectralogic.ds3client.MockedHeaders;
import com.spectralogic.ds3client.commands.interfaces.MetadataImpl;
import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.MetadataReceivedListener;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.events.MetadataEvent;
import com.spectralogic.ds3client.helpers.events.SameThreadEventRunner;

import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.networking.Metadata;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class EventDispatcherImpl_Test {
    @Test
    public void testAddingChecksumObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ChecksumObserver checksumObserver = new ChecksumObserver(new ChecksumListener() {
            @Override
            public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachChecksumObserver(checksumObserver);
        eventDispatcher.attachChecksumObserver(checksumObserver);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, "checksum");

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingChecksumObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ChecksumObserver checksumObserver = new ChecksumObserver(new ChecksumListener() {
            @Override
            public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        final ChecksumObserver checksumObserverId = eventDispatcher.attachChecksumObserver(checksumObserver);
        final ChecksumObserver checksumObserverId2 = eventDispatcher.attachChecksumObserver(checksumObserver);

        assertEquals(checksumObserverId, checksumObserverId2);

        eventDispatcher.removeChecksumObserver(checksumObserverId);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, "checksum");

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingDataTransferredObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);

        eventDispatcher.emitDataTransferredEvent(new BulkObject());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingDataTransferredObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        final DataTransferredObserver dataTransferredObserverId =  eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        final DataTransferredObserver dataTransferredObserverId2 = eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);

        assertEquals(dataTransferredObserverId, dataTransferredObserverId2);

        eventDispatcher.removeDataTransferredObserver(dataTransferredObserverId);

        eventDispatcher.emitDataTransferredEvent(new BulkObject());

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingFailureEventObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        final FailureEventObserver failureEventObserverId = eventDispatcher.attachFailureEventObserver(failureEventObserver);
        final FailureEventObserver failureEventObserverId2 = eventDispatcher.attachFailureEventObserver(failureEventObserver);

        assertEquals(failureEventObserverId, failureEventObserverId2);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed("object name")
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingFailureEventObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        final FailureEventObserver failureEventObserverId = eventDispatcher.attachFailureEventObserver(failureEventObserver);
        final FailureEventObserver failureEventObserverId2 = eventDispatcher.attachFailureEventObserver(failureEventObserver);

        assertEquals(failureEventObserverId, failureEventObserverId2);

        eventDispatcher.removeFailureEventObserver(failureEventObserverId);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed("object name")
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingObjectCompletedEventObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);

        eventDispatcher.emitObjectCompletedEvent(new BulkObject());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingObjectCompletedEventObserver() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        final ObjectCompletedObserver objectCompletedObserverId = eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        final ObjectCompletedObserver objectCompletedObserverId2 = eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);

        assertEquals(objectCompletedObserverId, objectCompletedObserverId2);

        eventDispatcher.removeObjectCompletedObserver(objectCompletedObserverId);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingWaitingForChunksEventObserver() {
        final int numSecondsToWait = 85;

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final WaitingForChunksObserver waitingForChunksObserver = new WaitingForChunksObserver(new WaitingForChunksListener() {
            @Override
            public void waiting(final int secondsToWait) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, secondsToWait);
            }
        });

        eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);
        eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingWaitingForChunksEventObserver() {
        final int numSecondsToWait = 85;

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final WaitingForChunksObserver waitingForChunksObserver = new WaitingForChunksObserver(new WaitingForChunksListener() {
            @Override
            public void waiting(final int secondsToWait) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, secondsToWait);
            }
        });

        final WaitingForChunksObserver waitingForChunksObserverId = eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);
        final WaitingForChunksObserver waitingForChunksObserverId2 = eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);

        assertEquals(waitingForChunksObserverId, waitingForChunksObserverId2);

        eventDispatcher.removeWaitingForChunksObserver(waitingForChunksObserverId);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingMetadataReceivedEventObserver() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetaDataReceivedObserver metaDataReceivedObserver = new MetaDataReceivedObserver(new MetadataReceivedListener() {
            @Override
            public void metadataReceived(final String filename, final Metadata metadata) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, filename);
                assertEquals(expectedMetadata, metadata);
            }
        });

        eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);
        eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingMetadataReceivedEventObserver() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetaDataReceivedObserver metaDataReceivedObserver = new MetaDataReceivedObserver(new MetadataReceivedListener() {
            @Override
            public void metadataReceived(final String filename, final Metadata metadata) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, filename);
                assertEquals(expectedMetadata, metadata);
            }
        });

        final MetaDataReceivedObserver metaDataReceivedObserverId = eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);
        final MetaDataReceivedObserver metaDataReceivedObserverId2 = eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);

        assertEquals(metaDataReceivedObserverId, metaDataReceivedObserverId2);

        eventDispatcher.removeMetadataReceivedEventObserver(metaDataReceivedObserverId);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedChecksumObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String checksumValue = "checksun";

        final ChecksumObserver checksumObserver = new ChecksumObserver(new UpdateStrategy<ChecksumEvent>() {
            @Override
            public void update(final ChecksumEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(checksumValue, eventData.getChecksum());
            }
        });

        eventDispatcher.attachChecksumObserver(checksumObserver);
        eventDispatcher.attachChecksumObserver(checksumObserver);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, checksumValue);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedChecksumObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String checksumValue = "checksun";

        final ChecksumObserver checksumObserver = new ChecksumObserver(new UpdateStrategy<ChecksumEvent>() {
            @Override
            public void update(final ChecksumEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(checksumValue, eventData.getChecksum());
            }
        });

        final ChecksumObserver checksumObserverId = eventDispatcher.attachChecksumObserver(checksumObserver);
        final ChecksumObserver checksumObserverId2 = eventDispatcher.attachChecksumObserver(checksumObserver);

        assertEquals(checksumObserverId, checksumObserverId2);

        eventDispatcher.removeChecksumObserver(checksumObserverId);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, checksumValue);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedDataTransferredObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final Long dataTransferredLength = 85L;

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new UpdateStrategy<Long>() {
            @Override
            public void update(final Long eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(dataTransferredLength, eventData);
            }
        });

        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);

        final BulkObject blob = new BulkObject();
        blob.setLength(dataTransferredLength);

        eventDispatcher.emitDataTransferredEvent(blob);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedDataTransferredObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final Long dataTransferredLength = 85L;

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new UpdateStrategy<Long>() {
            @Override
            public void update(final Long eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(dataTransferredLength, eventData);
            }
        });

        final DataTransferredObserver dataTransferredObserverId = eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        final DataTransferredObserver dataTransferredObserverId2 = eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);

        assertEquals(dataTransferredObserverId, dataTransferredObserverId2);

        eventDispatcher.removeDataTransferredObserver(dataTransferredObserverId);

        final BulkObject blob = new BulkObject();
        blob.setLength(dataTransferredLength);

        eventDispatcher.emitDataTransferredEvent(blob);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedFailureEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new UpdateStrategy<FailureEvent>() {
            @Override
            public void update(final FailureEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertNotNull(eventData.getCausalException());
                assertNotNull(eventData.doingWhat());
                assertNotNull(eventData.withObjectNamed());
            }
        });

        eventDispatcher.attachFailureEventObserver(failureEventObserver);
        eventDispatcher.attachFailureEventObserver(failureEventObserver);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed("object name")
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedFailureEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new UpdateStrategy<FailureEvent>() {
            @Override
            public void update(final FailureEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertNotNull(eventData.getCausalException());
                assertNotNull(eventData.doingWhat());
                assertNotNull(eventData.withObjectNamed());
            }
        });

        final FailureEventObserver failureEventObserverId = eventDispatcher.attachFailureEventObserver(failureEventObserver);
        final FailureEventObserver failureEventObserverId2 = eventDispatcher.attachFailureEventObserver(failureEventObserver);

        assertEquals(failureEventObserverId, failureEventObserverId2)
        ;
        eventDispatcher.removeFailureEventObserver(failureEventObserverId);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed("object name")
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedObjectCompletedEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String blobName = "Blobby";

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new UpdateStrategy<String>() {
            @Override
            public void update(final String eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(eventData, blobName);
            }
        });

        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);

        eventDispatcher.emitObjectCompletedEvent(blob);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedObjectCompletedEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String blobName = "Blobby";

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new UpdateStrategy<String>() {
            @Override
            public void update(final String eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(eventData, blobName);
            }
        });

        final ObjectCompletedObserver objectCompletedObserverId = eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        final ObjectCompletedObserver objectCompletedObserverId2 = eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);

        assertEquals(objectCompletedObserverId, objectCompletedObserverId2);

        eventDispatcher.removeObjectCompletedObserver(objectCompletedObserverId);

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);

        eventDispatcher.emitObjectCompletedEvent(blob);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedObjectWaitingForChunkEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final Integer numSecondsToWait = 5;

        final WaitingForChunksObserver waitingForChunksObserver = new WaitingForChunksObserver(new UpdateStrategy<Integer>() {
            @Override
            public void update(final Integer eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, eventData);
            }
        });

        eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);
        eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait.intValue());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedObjectWaitingForChunkEventObserverEventDispatcher() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final Integer numSecondsToWait = 5;

        final WaitingForChunksObserver waitingForChunksObserver = new WaitingForChunksObserver(new UpdateStrategy<Integer>() {
            @Override
            public void update(final Integer eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, eventData);
            }
        });

        final WaitingForChunksObserver waitingForChunksObserverId = eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);
        final WaitingForChunksObserver waitingForChunksObserverId2 = eventDispatcher.attachWaitingForChunksObserver(waitingForChunksObserver);

        assertEquals(waitingForChunksObserverId, waitingForChunksObserverId2);

        eventDispatcher.removeWaitingForChunksObserver(waitingForChunksObserverId);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait.intValue());

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedMetadataReceivedEventObserver() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetaDataReceivedObserver metaDataReceivedObserver = new MetaDataReceivedObserver(new UpdateStrategy<MetadataEvent>() {
            @Override
            public void update(final MetadataEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, eventData.getObjectName());
                assertEquals(expectedMetadata, eventData.getMetadata());
            }
        });

        eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);
        eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedMetadataReceivedEventObserver() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetaDataReceivedObserver metaDataReceivedObserver = new MetaDataReceivedObserver(new UpdateStrategy<MetadataEvent>() {
            @Override
            public void update(final MetadataEvent eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, eventData.getObjectName());
                assertEquals(expectedMetadata, eventData.getMetadata());
            }
        });

        final MetaDataReceivedObserver metaDataReceivedObserverId = eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);
        final MetaDataReceivedObserver metaDataReceivedObserverId2 = eventDispatcher.attachMetadataReceivedEventObserver(metaDataReceivedObserver);

        assertEquals(metaDataReceivedObserverId, metaDataReceivedObserverId2)
        ;
        eventDispatcher.removeMetadataReceivedEventObserver(metaDataReceivedObserverId);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingUserDefinedBlobTransferredEventObserver() {
        final String blobName = "Gracie";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final BlobTransferredEventObserver blobTransferredEventObserver = new BlobTransferredEventObserver(new UpdateStrategy<BulkObject>() {
            @Override
            public void update(final BulkObject eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(blobName, eventData.getName());
            }
        });

        eventDispatcher.attachBlobTransferredEventObserver(blobTransferredEventObserver);
        eventDispatcher.attachBlobTransferredEventObserver(blobTransferredEventObserver);

        eventDispatcher.emitBlobTransferredEvent(blob);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingUserDefinedBlobTransferredEventObserver() {
        final String blobName = "Gracie";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final BlobTransferredEventObserver blobTransferredEventObserver = new BlobTransferredEventObserver(new UpdateStrategy<BulkObject>() {
            @Override
            public void update(final BulkObject eventData) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(blobName, eventData.getName());
            }
        });

        final BlobTransferredEventObserver blobTransferredEventObserverId = eventDispatcher.attachBlobTransferredEventObserver(blobTransferredEventObserver);
        final BlobTransferredEventObserver blobTransferredEventObserverId2 = eventDispatcher.attachBlobTransferredEventObserver(blobTransferredEventObserver);

        assertEquals(blobTransferredEventObserverId, blobTransferredEventObserverId2);

        eventDispatcher.removeBlobTransferredEventObserver(blobTransferredEventObserverId);

        eventDispatcher.emitBlobTransferredEvent(blob);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingDataTransferredListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final long transferSize = 85L;

        final BulkObject blob = new BulkObject();
        blob.setName("Gracie");
        blob.setLength(transferSize);
        blob.setOffset(transferSize + 1);

        final DataTransferredListener dataTransferredListener = new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(transferSize, size);
            }
        };

        eventDispatcher.attachDataTransferredListener(dataTransferredListener);
        eventDispatcher.attachDataTransferredListener(dataTransferredListener);

        eventDispatcher.emitDataTransferredEvent(blob);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingDataTransferredListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final long transferSize = 85L;

        final BulkObject blob = new BulkObject();
        blob.setName("Gracie");
        blob.setLength(transferSize);
        blob.setOffset(transferSize + 1);

        final DataTransferredListener dataTransferredListener = new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(transferSize, size);
            }
        };

        eventDispatcher.attachDataTransferredListener(dataTransferredListener);
        eventDispatcher.attachDataTransferredListener(dataTransferredListener);
        eventDispatcher.removeDataTransferredListener(dataTransferredListener);

        eventDispatcher.emitDataTransferredEvent(blob);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingObjectCompletedListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String blobName = "Gracie";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);
        blob.setLength(0);
        blob.setOffset(1);

        final ObjectCompletedListener objectCompletedListener = new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(blob.getName(), name);
            }
        };

        eventDispatcher.attachObjectCompletedListener(objectCompletedListener);
        eventDispatcher.attachObjectCompletedListener(objectCompletedListener);

        eventDispatcher.emitObjectCompletedEvent(blob);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingObjectCompletedListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String blobName = "Gracie";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);
        blob.setLength(0);
        blob.setOffset(1);

        final ObjectCompletedListener objectCompletedListener = new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(blob.getName(), name);
            }
        };

        eventDispatcher.attachObjectCompletedListener(objectCompletedListener);
        eventDispatcher.attachObjectCompletedListener(objectCompletedListener);
        eventDispatcher.removeObjectCompletedListener(objectCompletedListener);

        eventDispatcher.emitObjectCompletedEvent(blob);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingMetadataReceivedListener() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetadataReceivedListener metadataReceivedListener = new MetadataReceivedListener() {
            @Override
            public void metadataReceived(final String filename, final Metadata metadata) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, filename);
                assertEquals(expectedMetadata, metadata);
            }
        };

        eventDispatcher.attachMetadataReceivedListener(metadataReceivedListener);
        eventDispatcher.attachMetadataReceivedListener(metadataReceivedListener);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingMetadataReceivedListener() {
        final String objectName = "Gracie";
        final Map<String, String> mockedHeaderContents = new HashMap<>();
        mockedHeaderContents.put("1st", "Trixie");
        mockedHeaderContents.put("2nd", "Shasta");
        mockedHeaderContents.put("3rd", "Gracie");
        final Metadata expectedMetadata = new MetadataImpl(new MockedHeaders(mockedHeaderContents));

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final MetadataReceivedListener metadataReceivedListener = new MetadataReceivedListener() {
            @Override
            public void metadataReceived(final String filename, final Metadata metadata) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(objectName, filename);
                assertEquals(expectedMetadata, metadata);
            }
        };

        eventDispatcher.attachMetadataReceivedListener(metadataReceivedListener);
        eventDispatcher.attachMetadataReceivedListener(metadataReceivedListener);
        eventDispatcher.removeMetadataReceivedListener(metadataReceivedListener);

        eventDispatcher.emitMetaDataReceivedEvent(objectName, expectedMetadata);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingChecksumListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String checksumText = "checksum";

        final ChecksumListener checksumListener = new ChecksumListener() {
            @Override
            public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                assertEquals(ChecksumType.Type.MD5, type);
                assertEquals(checksumText, checksum);
                numTimesHandlerCalled.getAndIncrement();
            }
        };

        eventDispatcher.attachChecksumListener(checksumListener);
        eventDispatcher.attachChecksumListener(checksumListener);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, checksumText);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingChecksumListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String checksumText = "checksum";

        final ChecksumListener checksumListener = new ChecksumListener() {
            @Override
            public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                assertEquals(ChecksumType.Type.MD5, type);
                assertEquals(checksumText, checksum);
                numTimesHandlerCalled.getAndIncrement();
            }
        };

        eventDispatcher.attachChecksumListener(checksumListener);
        eventDispatcher.attachChecksumListener(checksumListener);
        eventDispatcher.removeChecksumListener(checksumListener);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, checksumText);

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingFailureListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final String eventName = "Gracie";

        final FailureEventListener failureEventListener = new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(eventName, failureEvent.withObjectNamed());
            }
        };

        eventDispatcher.attachFailureEventListener(failureEventListener);
        eventDispatcher.attachFailureEventListener(failureEventListener);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed(eventName)
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingFailureListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventListener failureEventListener = new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
            }
        };

        eventDispatcher.attachFailureEventListener(failureEventListener);
        eventDispatcher.attachFailureEventListener(failureEventListener);
        eventDispatcher.removeFailureEventListener(failureEventListener);

        eventDispatcher.emitFailureEvent(new FailureEvent.Builder()
                .withObjectNamed("object name")
                .doingWhat(FailureEvent.FailureActivity.GettingObject)
                .withCausalException(new Exception())
                .usingSystemWithEndpoint("endpoint")
                .build());

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingWaitingForChunksListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final int numSecondsToWait = 85;

        final WaitingForChunksListener waitingForChunksListener = new WaitingForChunksListener() {
            @Override
            public void waiting(final int secondsToWait) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, secondsToWait);
            }
        };

        eventDispatcher.attachWaitingForChunksListener(waitingForChunksListener);
        eventDispatcher.attachWaitingForChunksListener(waitingForChunksListener);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait);

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingWaitingForChunksListener() {
        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner());

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final int numSecondsToWait = 85;

        final WaitingForChunksListener waitingForChunksListener = new WaitingForChunksListener() {
            @Override
            public void waiting(final int secondsToWait) {
                numTimesHandlerCalled.getAndIncrement();
                assertEquals(numSecondsToWait, secondsToWait);
            }
        };

        eventDispatcher.attachWaitingForChunksListener(waitingForChunksListener);
        eventDispatcher.attachWaitingForChunksListener(waitingForChunksListener);
        eventDispatcher.removeWaitingForChunksListener(waitingForChunksListener);

        eventDispatcher.emitWaitingForChunksEvents(numSecondsToWait);

        assertEquals(0, numTimesHandlerCalled.get());
    }
}
