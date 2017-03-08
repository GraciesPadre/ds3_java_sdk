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

package com.spectralogic.ds3client.helpers.strategy.channelstrategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.channels.RangedSeekableByteChannel;
import com.spectralogic.ds3client.helpers.channels.WindowedSeekableByteChannel;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.utils.Guard;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Map;

public class RandomAccessChannelStrategy implements ChannelStrategy {
    private final Object lock = new Object();

    private final Ds3ClientHelpers.ObjectChannelBuilder objectChannelBuilder;
    private final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs;
    private final ChannelPreparable channelPreparer;
    private final Map<BulkObject, SeekableByteChannel> blobChannelMap;


    public RandomAccessChannelStrategy(final Ds3ClientHelpers.ObjectChannelBuilder objectChannelBuilder,
                                       final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs,
                                       final ChannelPreparable channelPreparer)
    {
        Preconditions.checkNotNull(objectChannelBuilder, "objectChannelBuilder may not be null.");
        Preconditions.checkNotNull(channelPreparer, "channelPreparer may not be null.");

        this.objectChannelBuilder = objectChannelBuilder;
        this.rangesForBlobs = rangesForBlobs;
        this.channelPreparer = channelPreparer;
        blobChannelMap  = new HashMap<>();
    }

    @Override
    public SeekableByteChannel acquireChannelForBlob(final BulkObject blob) throws IOException {
        synchronized (lock) {
            channelPreparer.prepareChannel(blob.getName(), objectChannelBuilder);

            SeekableByteChannel seekableByteChannel = blobChannelMap.get(blob);

            if (seekableByteChannel == null) {
                seekableByteChannel = new RangedSeekableByteChannel(objectChannelBuilder.buildChannel(blob.getName()),
                        getRangesForABlob(blob),
                        blob.getName());
                blobChannelMap.put(blob, seekableByteChannel);
            }

            return new WindowedSeekableByteChannel(seekableByteChannel, lock, blob.getOffset(), blob.getLength());
        }
    }

    private ImmutableMultimap<BulkObject, Range> getRangesForABlob(final BulkObject blob) {
        if (Guard.isMapNullOrEmpty(rangesForBlobs)) {
            return ImmutableMultimap.of();
        }

        final ImmutableMultimap<BulkObject, Range> rangesForABlob = rangesForBlobs.get(blob.getName());

        if (Guard.isMultiMapNullOrEmpty(rangesForABlob)) {
            return ImmutableMultimap.of();
        }

        return rangesForABlob;
    }

    @Override
    public void releaseChannelForBlob(final SeekableByteChannel seekableByteChannel, final BulkObject blob) throws IOException {
        synchronized (lock) {
            final SeekableByteChannel cachedChannel = blobChannelMap.remove(blob);

            Throwable closingCachedChannelException = null;

            try {
                cachedChannel.close();
            } catch (final Throwable t) {
                closingCachedChannelException = t;
            }

            try {
                seekableByteChannel.close();
            } catch (final Throwable t) {
                throw new IOException(t);
            }

            if (closingCachedChannelException != null) {
                throw new IOException(closingCachedChannelException);
            }
        }
    }
}
