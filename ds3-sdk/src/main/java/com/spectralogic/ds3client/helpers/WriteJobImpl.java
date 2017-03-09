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
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class WriteJobImpl extends JobImpl {
    static private final Logger LOG = LoggerFactory.getLogger(WriteJobImpl.class);

    private Ds3ClientHelpers.MetadataAccess metadataAccess = null;
    private ChecksumFunction checksumFunction = null;

    public WriteJobImpl(final TransferStrategyBuilder transferStrategyBuilder,
                        final Ds3Client client,
                        final MasterObjectList masterObjectList,
                        final EventDispatcher eventDispatcher)
    {
        super(transferStrategyBuilder, client, masterObjectList, eventDispatcher);
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

            transferStrategyBuilder().withChecksumFunction(checksumFunction);
            transferStrategyBuilder().withMetadataAccess(metadataAccess);

            try {
                final JobState jobState = transferStrategyBuilder().makeJobStateForPutJob();

                try (final TransferStrategy transferStrategy = transferStrategyBuilder().makeOriginalSdkSemanticsPutTransferStrategy()) {
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
}

