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
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.GetObjectResponse;
import com.spectralogic.ds3client.helpers.ChunkTransferrer.ItemTransferrer;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.ObjectChannelBuilder;
import com.spectralogic.ds3client.helpers.events.EventRunner;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.GetSequentialBlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.SequentialFileWriterChannelStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcher;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.EventDispatcherImpl;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.MetaDataReceivedObserver;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategy;
import com.spectralogic.ds3client.helpers.strategy.transferstrategy.TransferStrategyBuilder;
import com.spectralogic.ds3client.helpers.util.PartialObjectHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.networking.Metadata;
import com.spectralogic.ds3client.utils.Guard;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

class ReadJobImpl extends JobImpl {

    private final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> blobToRanges;
    private final Set<MetadataReceivedListener> metadataListeners;
    private final TransferStrategyBuilder transferStrategyBuilder;

    public ReadJobImpl(final TransferStrategyBuilder transferStrategyBuilder,
                       final Ds3Client client,
                       final MasterObjectList masterObjectList,
                       final ImmutableMultimap<String, Range> objectRanges,
                       final int objectTransferAttempts,
                       final EventRunner eventRunner,
                       final EventDispatcher eventDispatcher)
    {
        super(client, masterObjectList, objectTransferAttempts, eventRunner, eventDispatcher);

        this.blobToRanges = PartialObjectHelpers.mapRangesToBlob(masterObjectList.getObjects(), objectRanges);
        this.metadataListeners = Sets.newIdentityHashSet();
        this.transferStrategyBuilder = transferStrategyBuilder;
    }

    protected static ImmutableList<BulkObject> getAllBlobApiBeans(final List<Objects> jobWithChunksApiBeans) {
        final ImmutableList.Builder<BulkObject> builder = ImmutableList.builder();
        for (final Objects objects : jobWithChunksApiBeans) {
            builder.addAll(objects.getObjects());
        }
        return builder.build();
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

            transferStrategyBuilder.withChannelBuilder(channelBuilder);
            transferStrategyBuilder.withJobPartTracker(getJobPartTracker());

            try (final JobState jobState = new JobState(
                    channelBuilder,
                    this.masterObjectList.getObjects(),
                    getJobPartTracker(), blobToRanges)) {
                try (final TransferStrategy transferStrategy = transferStrategyBuilder.makeOriginalSdkSemanticsGetTransferStrategy()) {
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
            throw new RuntimeException(t);
        }
    }

    @Override
    protected List<Objects> getChunks(final MasterObjectList masterObjectList) {
        return masterObjectList.getObjects();
    }

    @Override
    protected JobPartTrackerDecorator makeJobPartTracker(final List<Objects> chunks, final EventRunner eventRunner) {
        final JobPartTrackerDecorator result = new JobPartTrackerDecorator(chunks, eventRunner);

        result.attachObjectCompletedListener(new ObjectCompletedListener() {
            @Override
            public void objectCompleted(final String name) {
                getEventDispatcher().emitObjectCompletedEvent(name);
            }
        });

        return result;
    }

    private final class GetObjectTransferrerRetryDecorator implements ItemTransferrer {
        private final GetObjectTransferrer getObjectTransferrer;

        private GetObjectTransferrerRetryDecorator(final JobState jobState) {
            getObjectTransferrer = new GetObjectTransferrer(jobState);
        }

        @Override
        public void transferItem(final JobPart jobPart, final BulkObject ds3Object) throws IOException {
            ReadJobImpl.this.transferItem(jobPart, client, ds3Object, getObjectTransferrer);
        }
    }

    private final class GetObjectTransferrer implements ItemTransferrer {
        private final JobState jobState;

        private GetObjectTransferrer(final JobState jobState) {
            this.jobState = jobState;
        }

        @Override
        public void transferItem(final JobPart jobPart, final BulkObject ds3Object)
                throws IOException {

            final ImmutableCollection<Range> ranges = getRangesForBlob(blobToRanges, ds3Object);

            final GetObjectRequest request = new GetObjectRequest(
                    ReadJobImpl.this.masterObjectList.getBucketName(),
                    ds3Object.getName(),
                    jobState.getChannel(ds3Object.getName(), ds3Object.getOffset(), ds3Object.getLength()),
                    ReadJobImpl.this.getJobId().toString(),
                    ds3Object.getOffset()
            );

            if (Guard.isNotNullAndNotEmpty(ranges)) {
                request.withByteRanges(ranges);
            }

            final GetObjectResponse response = client.getObject(request);
            final Metadata metadata = response.getMetadata();

            emitChecksumEvents(ds3Object, response.getChecksumType(), response.getChecksum());
            sendMetadataEvents(ds3Object.getName(), metadata);
        }
    }

    private void sendMetadataEvents(final String objName , final Metadata metadata) {
        for (final MetadataReceivedListener listener : this.metadataListeners) {
            getEventRunner().emitEvent(new Runnable() {
                @Override
                public void run() {
                    listener.metadataReceived(objName, metadata);
                }
            });
        }
    }

    private static ImmutableCollection<Range> getRangesForBlob(
            final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> blobToRanges,
            final BulkObject ds3Object) {
        final ImmutableMultimap<BulkObject, Range> ranges =  blobToRanges.get(ds3Object.getName());
        if (ranges == null) return null;
        return ranges.get(ds3Object);
    }
}
