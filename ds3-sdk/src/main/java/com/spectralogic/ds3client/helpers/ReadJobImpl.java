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

import com.google.common.collect.*;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.MetaDataReceivedObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.helpers.util.PartialObjectHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.common.Range;

import java.io.IOException;
import java.util.List;

class ReadJobImpl extends JobImpl {

    private final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs;

    public ReadJobImpl(final TransferStrategyBuilder transferStrategyBuilder,
                       final Ds3Client client,
                       final MasterObjectList masterObjectList,
                       final ImmutableMultimap<String, Range> objectRanges,
                       final EventRunner eventRunner,
                       final EventDispatcher eventDispatcher)
    {
        super(transferStrategyBuilder, client, masterObjectList, eventRunner, eventDispatcher);

        this.rangesForBlobs = PartialObjectHelpers.mapRangesToBlob(masterObjectList.getObjects(), objectRanges);
    }

    @Override
    public void attachMetadataReceivedListener(final MetadataReceivedListener listener) {
        checkRunning();
        getEventDispatcher().attachMetadataReceivedEventObserver(new MetaDataReceivedObserver(listener));
    }

    @Override
    public void removeMetadataReceivedListener(final MetadataReceivedListener listener) {
        checkRunning();
        getEventDispatcher().removeMetadataReceivedEventObserver(new MetaDataReceivedObserver(listener));
    }

    @Override
    public Ds3ClientHelpers.Job withMetadata(final Ds3ClientHelpers.MetadataAccess access) {
        throw new IllegalStateException("withMetadata method is not used with Read Jobs");
    }

    @Override
    public Ds3ClientHelpers.Job withChecksum(final ChecksumFunction checksumFunction) {
        throw new IllegalStateException("withChecksum is not supported on Read Jobs");
    }

    @Override
    public void transfer(final ObjectChannelBuilder channelBuilder)
            throws IOException {
        try {
            running = true;

            super.transfer(channelBuilder);

            getTransferStrategyBuilder().withRangesForBlobs(rangesForBlobs);

            try {
                final JobState jobState = new JobState(this.masterObjectList.getObjects(), getJobPartTracker());

                try (final TransferStrategy transferStrategy = getTransferStrategyBuilder().makeOriginalSdkSemanticsGetTransferStrategy()) {
                    while (jobState.hasObjects()) {
                        transferStrategy.transfer();
                    }
                }
            } catch (final RuntimeException | IOException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } catch (final Throwable t) {
            emitFailureEvent(makeFailureEvent(FailureEvent.FailureActivity.GettingObject, t, masterObjectList.getObjects().get(0)));
            throw t;
        }
    }

    @Override
    protected List<Objects> getChunks(final MasterObjectList masterObjectList) {
        return masterObjectList.getObjects();
    }

    @Override
    protected JobPartTracker makeJobPartTracker(final List<Objects> chunks, final EventRunner eventRunner) {
        final JobPartTracker result = JobPartTrackerFactory.buildPartTracker(getAllBlobApiBeans(chunks), eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                getEventDispatcher().emitObjectCompletedEvent(name);
            }
        });

        return result;
    }
}
