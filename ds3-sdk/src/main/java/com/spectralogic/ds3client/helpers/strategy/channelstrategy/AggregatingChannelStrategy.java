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
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.spectralogic.ds3client.models.BulkObject;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.util.Set;

public class AggregatingChannelStrategy implements ChannelStrategy {
    private final Object lock = new Object();

    private final SetMultimap<String, OffsetLengthChannelTriple> blobOffsetLengthMap =
            Multimaps.synchronizedSetMultimap(HashMultimap.<String, OffsetLengthChannelTriple>create());

    private final ChannelStrategy channelStrategyDelegate;

    public AggregatingChannelStrategy(final ChannelStrategy channelStrategy) {
        channelStrategyDelegate = channelStrategy;
    }

    @Override
    public BlobChannelPair acquireChannelForBlob(final BulkObject blob) throws IOException {
        synchronized (lock) {
            return blobOffsetLengthMap.containsKey(blob.getName()) ? getExistingBlobChannelPair(blob) : makeNewBlobChannelPair(blob);
        }
    }

    private BlobChannelPair getExistingBlobChannelPair(final BulkObject blob) {
        final String blobName = blob.getName();

        final Set<OffsetLengthChannelTriple> offsetLengthChannelTriples = blobOffsetLengthMap.get(blobName);
        final ByteChannel channel = offsetLengthChannelTriples.iterator().next().getChannel();
        blobOffsetLengthMap.put(blobName, new OffsetLengthChannelTriple(blob.getOffset(), blob.getLength(), channel));

        return new BlobChannelPair(blob, channel);
    }

    private BlobChannelPair makeNewBlobChannelPair(final BulkObject blob) throws IOException {
        final BlobChannelPair blobChannelPair = channelStrategyDelegate.acquireChannelForBlob(blob);

        blobOffsetLengthMap.put(
                blob.getName(),
                new OffsetLengthChannelTriple(
                        blob.getOffset(),
                        blob.getLength(),
                        blobChannelPair.getChannel()));

        return blobChannelPair;
    }

    @Override
    public BlobChannelPair releaseChannelForBlob(final BlobChannelPair blobChannelPair) throws IOException {
        synchronized (lock) {
            final String blobName = blobChannelPair.getBlob().getName();

            blobOffsetLengthMap.remove(
                    blobName,
                    new OffsetLengthChannelTriple(
                            blobChannelPair.getBlob().getOffset(),
                            blobChannelPair.getBlob().getLength(),
                            blobChannelPair.getChannel()));

            if (blobOffsetLengthMap.containsKey(blobName)) {
                return new BlobChannelPair(blobChannelPair.getBlob(), blobChannelPair.getChannel());
            }

            return channelStrategyDelegate.releaseChannelForBlob(blobChannelPair);
        }
    }

    private static final class OffsetLengthChannelTriple {
        final long offset;
        final long length;
        final ByteChannel channel;

        private OffsetLengthChannelTriple(final long offset, final long length, final ByteChannel channel) {
            this.offset = offset;
            this.length = length;
            this.channel = channel;
        }

        private ByteChannel getChannel() {
            return channel;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof OffsetLengthChannelTriple)) return false;

            final OffsetLengthChannelTriple that = (OffsetLengthChannelTriple) o;

            if (offset != that.offset) return false;
            return length == that.length;
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + (int) (length ^ (length >>> 32));
            return result;
        }
    }
}
