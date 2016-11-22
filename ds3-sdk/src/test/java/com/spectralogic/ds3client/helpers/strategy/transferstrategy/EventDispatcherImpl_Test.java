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

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.events.SameThreadEventRunner;

import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class EventDispatcherImpl_Test {
    @Test
    public void testAddingChecksumObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

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
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ChecksumObserver checksumObserver = new ChecksumObserver(new ChecksumListener() {
            @Override
            public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachChecksumObserver(checksumObserver);
        eventDispatcher.attachChecksumObserver(checksumObserver);
        eventDispatcher.removeChecksumObserver(checksumObserver);

        eventDispatcher.emitChecksumEvent(new BulkObject(), ChecksumType.Type.MD5, "checksum");

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingDataTransferredObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);

        jobPartTracker.completePart("key", new ObjectPart(0, 0));

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingDataTransferredObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final DataTransferredObserver dataTransferredObserver = new DataTransferredObserver(new DataTransferredListener() {
            @Override
            public void dataTransferred(final long size) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        eventDispatcher.attachDataTransferredObserver(dataTransferredObserver);
        eventDispatcher.removeDataTransferredObserver(dataTransferredObserver);

        jobPartTracker.completePart("key", new ObjectPart(0, 0));

        assertEquals(0, numTimesHandlerCalled.get());
    }

    @Test
    public void testAddingFailureEventObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
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
    public void testRemovingFailureEventObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final FailureEventObserver failureEventObserver = new FailureEventObserver(new FailureEventListener() {
            @Override
            public void onFailure(final FailureEvent failureEvent) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachFailureEventObserver(failureEventObserver);
        eventDispatcher.attachFailureEventObserver(failureEventObserver);
        eventDispatcher.removeFailureEventObserver(failureEventObserver);

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
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);

        jobPartTracker.completePart("key", new ObjectPart(0, 0));

        assertEquals(1, numTimesHandlerCalled.get());
    }

    @Test
    public void testRemovingObjectCompletedEventObserver() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

        final AtomicInteger numTimesHandlerCalled = new AtomicInteger(0);

        final ObjectCompletedObserver objectCompletedObserver = new ObjectCompletedObserver(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                numTimesHandlerCalled.getAndIncrement();
            }
        });

        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        eventDispatcher.attachObjectCompletedObserver(objectCompletedObserver);
        eventDispatcher.removeObjectCompletedObserver(objectCompletedObserver);

        jobPartTracker.completePart("key", new ObjectPart(0, 0));

        assertEquals(0, numTimesHandlerCalled.get());
    }

    private static class MockJobPartTracker implements JobPartTracker {
        private final Set<DataTransferredListener> dataTransferredListeners = Sets.newIdentityHashSet();
        private final Set<ObjectCompletedListener> objectCompletedListeners = Sets.newIdentityHashSet();

        @Override
        public void completePart(final String key, final ObjectPart objectPart) {
            for (final DataTransferredListener dataTransferredListener : dataTransferredListeners) {
                dataTransferredListener.dataTransferred(0);
            }

            for (final ObjectCompletedListener objectCompletedListener : objectCompletedListeners) {
                objectCompletedListener.objectCompleted("object name");
            }
        }

        @Override
        public boolean containsPart(final String key, final ObjectPart objectPart) {
            return true;
        }

        @Override
        public JobPartTracker attachDataTransferredListener(final DataTransferredListener listener) {
            dataTransferredListeners.add(listener);
            return this;
        }

        @Override
        public JobPartTracker attachObjectCompletedListener(final ObjectCompletedListener listener) {
            objectCompletedListeners.add(listener);
            return this;
        }

        @Override
        public void removeDataTransferredListener(final DataTransferredListener listener) {
            dataTransferredListeners.remove(listener);
        }

        @Override
        public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
            objectCompletedListeners.remove(listener);
        }
    }

    @Test
    public void testAddingUserDefinedChecksumObserverEventDispatcher() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

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
    public void testAddingUserDefinedDataTransferredObserverEventDispatcher() {
        final JobPartTracker jobPartTracker = new MockJobPartTracker();

        final EventDispatcher eventDispatcher = new EventDispatcherImpl(new SameThreadEventRunner(), jobPartTracker);

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

        eventDispatcher.emitDataTransferredEvent(new BulkObject());
    }
}
