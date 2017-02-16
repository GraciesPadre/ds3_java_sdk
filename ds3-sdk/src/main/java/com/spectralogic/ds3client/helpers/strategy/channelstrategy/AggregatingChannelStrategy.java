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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.spectralogic.ds3client.models.BulkObject;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Map;

public class AggregatingChannelStrategy implements ChannelStrategy {
    private final Object lock = new Object();

    private final SetMultimap<String, Long> blobNameOffsetMap = HashMultimap.create();
    private final Map<String, SeekableByteChannel> blobNameChannelMap = new HashMap<>();

    private final ChannelStrategy channelStrategyDelegate;

    public AggregatingChannelStrategy(final ChannelStrategy channelStrategy) {
        channelStrategyDelegate = channelStrategy;
    }

    @Override
    public SeekableByteChannel acquireChannelForBlob(final BulkObject blob) throws IOException {
        final long offset = 0;
        return acquireChannelForBlob(blob, offset);
    }

    @Override
    public SeekableByteChannel acquireChannelForBlob(final BulkObject blob, final long offset) throws IOException {
        synchronized (lock) {
            final String blobName = blob.getName();

            blobNameOffsetMap.put(blobName, blob.getOffset());

            SeekableByteChannel seekableByteChannel = blobNameChannelMap.get(blobName);

            if (seekableByteChannel != null) {
                seekableByteChannel = setChannelPositionOrMakeNewChannel(blob, seekableByteChannel, offset);
            } else {
                seekableByteChannel = makeNewChannel(blob, offset);
            }

            return seekableByteChannel;
        }
    }

    private SeekableByteChannel setChannelPositionOrMakeNewChannel(final BulkObject blob,
                                                                   final SeekableByteChannel seekableByteChannel,
                                                                   final long offset)
        throws IOException
    {
        try {
            seekableByteChannel.position(offset);
            return seekableByteChannel;
        } catch (final ClosedChannelException e) {
            releaseChannelForBlob(seekableByteChannel, blob);
            return makeNewChannel(blob, offset);
        }
    }

    private SeekableByteChannel makeNewChannel(final BulkObject blob, final long offset) throws IOException {
        final SeekableByteChannel seekableByteChannel = channelStrategyDelegate.acquireChannelForBlob(blob, offset);

        blobNameChannelMap.put(blob.getName(), seekableByteChannel);

        return seekableByteChannel;
    }

    @Override
    public void releaseChannelForBlob(final SeekableByteChannel seekableByteChannel, final BulkObject blob) throws IOException {
        synchronized (lock) {
            final String blobName = blob.getName();

            blobNameOffsetMap.remove(blobName, blob.getOffset());

            if (blobNameOffsetMap.get(blobName).size() == 0) {
                blobNameChannelMap.remove(blobName);
                channelStrategyDelegate.releaseChannelForBlob(seekableByteChannel, blob);
            }
        }
    }
}