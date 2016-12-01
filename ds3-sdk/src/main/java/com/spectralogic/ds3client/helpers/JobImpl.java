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

package com.spectralogic.ds3client.helpers;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.Job;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.ChecksumObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.DataTransferredObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcherImpl;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.FailureEventObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.ObjectCompletedObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.WaitingForChunksObserver;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.MasterObjectList;
import com.spectralogic.ds3client.models.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.spectralogic.ds3client.helpers.ReadJobImpl.getAllBlobApiBeans;

abstract class JobImpl implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(JobImpl.class);

    protected final Ds3Client client;
    protected final MasterObjectList masterObjectList;
    protected boolean running = false;
    protected int maxParallelRequests = 10;
    private final int objectTransferAttempts;

    // TODO Get rid of this when transfer strategy completely replaces transferer
    private final EventRunner eventRunner;

    private final JobPartTracker jobPartTracker;
    private final EventDispatcher eventDispatcher;

    public JobImpl(final Ds3Client client,
                   final MasterObjectList masterObjectList,
                   final int objectTransferAttempts,
                   final EventRunner eventRunner) {
        this.client = client;
        this.masterObjectList = masterObjectList;
        this.objectTransferAttempts = objectTransferAttempts;
        this.eventRunner = eventRunner;

        jobPartTracker = makeJobPartTracker(getChunks(masterObjectList), eventRunner);

        eventDispatcher = new EventDispatcherImpl(eventRunner);
    }
    
    @Override
    public UUID getJobId() {
        if (this.masterObjectList == null) {
            return null;
        }
        return this.masterObjectList.getJobId();
    }

    @Override
    public String getBucketName() {
        if (this.masterObjectList == null) {
            return null;
        }
        return this.masterObjectList.getBucketName();
    }
    
    @Override
    public Job withMaxParallelRequests(final int maxParallelRequests) {
        this.maxParallelRequests = maxParallelRequests;
        return this;
    }

    protected void checkRunning() {
        if (running) throw new IllegalStateException("You cannot modify a job after calling transfer");
    }

    protected void transferItem(
            final JobPart jobPart,
            final Ds3Client client,
            final BulkObject ds3Object,
            final ChunkTransferrer.ItemTransferrer itemTransferrer)
            throws IOException
    {
        int objectTransfersAttempted = 0;

        while(true) {
            try {
                itemTransferrer.transferItem(jobPart, ds3Object);
                break;
            } catch (final Throwable t) {
                if (ExceptionClassifier.isUnrecoverableException(t) || ++objectTransfersAttempted >= getObjectTransferAttempts()) {
                    throw t;
                }
            }
        }
    }

    protected int getObjectTransferAttempts() {
        return objectTransferAttempts;
    }

    @Override
    public void attachChecksumListener(final ChecksumListener listener) {
        checkRunning();
        eventDispatcher.attachChecksumObserver(new ChecksumObserver(listener));
    }

    @Override
    public void removeChecksumListener(final ChecksumListener listener) {
        checkRunning();
        eventDispatcher.removeChecksumObserver(new ChecksumObserver(listener));
    }

    @Override
    public void attachWaitingForChunksListener(final WaitingForChunksListener listener) {
        checkRunning();
        eventDispatcher.attachWaitingForChunksObserver(new WaitingForChunksObserver(listener));
    }

    @Override
    public void removeWaitingForChunksListener(final WaitingForChunksListener listener) {
        checkRunning();
        eventDispatcher.removeWaitingForChunksObserver(new WaitingForChunksObserver(listener));
    }

    @Override
    public void attachFailureEventListener(final FailureEventListener listener) {
        checkRunning();
        eventDispatcher.attachFailureEventObserver(new FailureEventObserver(listener));
    }

    @Override
    public void removeFailureEventListener(final FailureEventListener listener) {
        checkRunning();
        eventDispatcher.removeFailureEventObserver(new FailureEventObserver(listener));
    }

    @Override
    public void attachDataTransferredListener(final DataTransferredListener listener) {
        checkRunning();
        eventDispatcher.attachDataTransferredObserver(new DataTransferredObserver(listener));
    }

    @Override
    public void removeDataTransferredListener(final DataTransferredListener listener) {
        checkRunning();
        eventDispatcher.removeDataTransferredObserver(new DataTransferredObserver(listener));
    }

    @Override
    public void attachObjectCompletedListener(final ObjectCompletedListener listener) {
        checkRunning();
        eventDispatcher.attachObjectCompletedObserver(new ObjectCompletedObserver(listener));
    }

    @Override
    public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
        checkRunning();
        eventDispatcher.removeObjectCompletedObserver(new ObjectCompletedObserver(listener));
    }

    protected void emitFailureEvent(final FailureEvent failureEvent) {
        eventDispatcher.emitFailureEvent(failureEvent);
    }

    protected FailureEvent makeFailureEvent(final FailureEvent.FailureActivity failureActivity,
                                            final Throwable causalException,
                                            final Objects chunk)
    {
        return new FailureEvent.Builder()
                .doingWhat(failureActivity)
                .withCausalException(causalException)
                .withObjectNamed(getLabelForChunk(chunk))
                .usingSystemWithEndpoint(client.getConnectionDetails().getEndpoint())
                .build();
    }

    protected String getLabelForChunk(final Objects chunk) {
        try {
            return chunk.getObjects().get(0).getName();
        } catch (final Throwable t) {
            LOG.error("Failed to get label for chunk.", t);
        }

        return "unnamed object";
    }

    protected void emitWaitingForChunksEvents(final int secondsToDelay) {
        eventDispatcher.emitWaitingForChunksEvents(secondsToDelay);
    }

    protected void emitChecksumEvents(final BulkObject bulkObject, final ChecksumType.Type checksumType, final String checksum) {
        eventDispatcher.emitChecksumEvent(bulkObject, checksumType, checksum);
    }

    protected abstract List<Objects> getChunks(final MasterObjectList masterObjectList);
    protected abstract JobPartTrackerDecorator makeJobPartTracker(final List<Objects> chunks, final EventRunner eventRunner);

    protected static class JobPartTrackerDecorator implements JobPartTracker {
        private final JobPartTracker clientJobPartTracker;
        private final JobPartTracker internalJobPartTracker;

        protected JobPartTrackerDecorator(final List<Objects> chunks, final EventRunner eventRunner) {
            clientJobPartTracker = JobPartTrackerFactory.buildPartTracker(getAllBlobApiBeans(chunks), eventRunner);
            internalJobPartTracker = JobPartTrackerFactory.buildPartTracker(getAllBlobApiBeans(chunks), eventRunner);
        }

        @Override
        public void completePart(final String key, final ObjectPart objectPart) {
            // It's important to fire the internal completions -- those we set up to close channels we
            // have opened -- before firing client-registered events.  The reason is that some clients
            // rely upon this ordering to know that channels are closed when their event handlers run.
            internalJobPartTracker.completePart(key, objectPart);
            clientJobPartTracker.completePart(key, objectPart);
        }

        @Override
        public boolean containsPart(final String key, final ObjectPart objectPart) {
            return internalJobPartTracker.containsPart(key, objectPart) || clientJobPartTracker.containsPart(key, objectPart);
        }

        @Override
        public JobPartTracker attachDataTransferredListener(final DataTransferredListener listener) {
            return clientJobPartTracker.attachDataTransferredListener(listener);
        }

        @Override
        public JobPartTracker attachObjectCompletedListener(final ObjectCompletedListener listener) {
            internalJobPartTracker.attachObjectCompletedListener(listener);
            return this;
        }

        @Override
        public void removeDataTransferredListener(final DataTransferredListener listener) {
            clientJobPartTracker.removeDataTransferredListener(listener);
        }

        @Override
        public void removeObjectCompletedListener(final ObjectCompletedListener listener) {
            internalJobPartTracker.removeObjectCompletedListener(listener);
        }

        protected void attachClientObjectCompletedListener(final ObjectCompletedListener listener) {
            clientJobPartTracker.attachObjectCompletedListener(listener);
        }

        protected void removeClientObjectCompletedListener(final ObjectCompletedListener listener) {
            clientJobPartTracker.removeObjectCompletedListener(listener);
        }
    }

    protected EventDispatcher getEventDispatcher() {
        return eventDispatcher;
    }

    protected EventRunner getEventRunner() {
        return eventRunner;
    }

    protected JobPartTracker getJobPartTracker() {
        return jobPartTracker;
    }
}
