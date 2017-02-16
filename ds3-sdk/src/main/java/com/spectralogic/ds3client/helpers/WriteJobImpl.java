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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.common.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.filterChunks;

class WriteJobImpl extends JobImpl {
    static private final Logger LOG = LoggerFactory.getLogger(WriteJobImpl.class);

    private List<Objects> filteredChunks;

    private Ds3ClientHelpers.MetadataAccess metadataAccess = null;
    private ChecksumFunction checksumFunction = null;

    // TODO: Get rid of all the ctor args other transferStrategy when super no longer needs them
    // TODO: May need to continue calling out event dispatcher separately.  Part of the job interface
    // allows for attaching events before the user calls startWriteJob, which is what causes the
    // creation of JobImpl.  May be have a jbb event dispatcher registrar?  The job id is in the master
    // object list.
    public WriteJobImpl(final TransferStrategyBuilder transferStrategyBuilder,
                        final Ds3Client client,
                        final MasterObjectList masterObjectList,
                        final EventRunner eventRunner,
                        final EventDispatcher eventDispatcher)
    {
        super(transferStrategyBuilder, client, masterObjectList, eventRunner, eventDispatcher);
    }

    @Override
    public void attachMetadataReceivedListener(final MetadataReceivedListener listener) {
        throw new IllegalStateException("Metadata listeners are not used with Write jobs");
    }

    @Override
    public void removeMetadataReceivedListener(final MetadataReceivedListener listener) {
        throw new IllegalStateException("Metadata listeners are not used with Write jobs");
    }

    @Override
    public Ds3ClientHelpers.Job withMetadata(final Ds3ClientHelpers.MetadataAccess access) {
        checkRunning();
        this.metadataAccess = access;
        return this;
    }

    @Override
    public Ds3ClientHelpers.Job withChecksum(final ChecksumFunction checksumFunction) {
        this.checksumFunction = checksumFunction;
        return this;
    }

    @Override
    public void transfer(final ObjectChannelBuilder channelBuilder) throws IOException {
        try {
            running = true;

            super.transfer(channelBuilder);

            LOG.debug("Starting job transfer");

            getTransferStrategyBuilder().withChecksumFunction(checksumFunction);
            getTransferStrategyBuilder().withMetadataAccess(metadataAccess);

            try (final JobState jobState = new JobState(
                    channelBuilder,
                    filteredChunks,
                    getJobPartTracker(),
                    ImmutableMap.<String, ImmutableMultimap<BulkObject, Range>>of())) {
                try (final TransferStrategy transferStrategy = getTransferStrategyBuilder().makeOriginalSdkSemanticsPutTransferStrategy()) {
                    while (jobState.hasObjects()) {
                        transferStrategy.transfer();
                    }
                }

            } catch (final IOException | RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } catch (final Throwable t) {
            emitFailureEvent(makeFailureEvent(FailureEvent.FailureActivity.PuttingObject, t, masterObjectList.getObjects().get(0)));
            throw t;
        }
    }

    @Override
    protected List<Objects> getChunks(final MasterObjectList masterObjectList) {
        if (masterObjectList == null || masterObjectList.getObjects() == null) {
            LOG.info("Job has no data to transfer");
            return null;
        }

        LOG.info("Ready to start transfer for job {} with {} chunks", masterObjectList.getJobId().toString(), masterObjectList.getObjects().size());

        filteredChunks = filterChunks(masterObjectList.getObjects());

        return filteredChunks;
    }

    @Override
    protected JobPartTracker makeJobPartTracker(final List<Objects> chunks, final EventRunner eventRunner) {
        if (chunks == null) {
            return null;
        }

        final JobPartTracker result = JobPartTrackerFactory.buildPartTracker(Iterables.concat(getAllBlobApiBeans(filteredChunks)), eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                getEventDispatcher().emitObjectCompletedEvent(name);
            }
        });

        return result;
    }
}

