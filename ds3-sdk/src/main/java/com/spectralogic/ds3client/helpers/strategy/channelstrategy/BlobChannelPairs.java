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

import com.google.common.collect.ImmutableMap;
import com.spectralogic.ds3client.models.BulkObject;

import java.nio.channels.ByteChannel;
import java.util.HashSet;
import java.util.Set;

public class BlobChannelPairs {
    private ImmutableMap<String, BlobChannelPair> blobChannelPairs;

    private BlobChannelPairsState blobChannelPairsState = new InitialState();

    public void addChannelForBlob(final BulkObject blob, final ByteChannel byteChannel) {
        blobChannelPairsState.addChannelForBlob(blob, byteChannel);
    }

    public ByteChannel channelForBlob(final BulkObject blob) {
        return blobChannelPairsState.channelForBlob(blob);
    }

    boolean containsBlob(final BulkObject blob) {
        return blobChannelPairsState.containsBlob(blob);
    }

    private BlobChannelPairsState transitionToState(final BlobChannelPairsState nextState) {
        blobChannelPairsState = nextState;
        return blobChannelPairsState;
    }

    private interface BlobChannelPairsState {
        void addChannelForBlob(final BulkObject blob, final ByteChannel byteChannel);
        boolean containsBlob(final BulkObject blob);
        ByteChannel channelForBlob(final BulkObject blob);
    }

    private class InitialState extends AbstractBlobChannelPairsState {
        private final ImmutableMap.Builder<String, BlobChannelPair> mapBuilder = new ImmutableMap.Builder<>();
        private final Set<String> blobNames = new HashSet<>();

        @Override
        public void addChannelForBlob(final BulkObject blob, final ByteChannel byteChannel) {
            final String blobUniqueName = blob.getUniqueName();

            if ( ! blobNames.contains(blobUniqueName)) {
                blobNames.add(blobUniqueName);
                mapBuilder.put(blobUniqueName, new BlobChannelPair(blob, byteChannel));
            }
        }

        @Override
        public boolean containsBlob(final BulkObject blob) {
            return blobNames.contains(blob.getUniqueName());
        }

        @Override
        public ByteChannel channelForBlob(final BulkObject blob) {
            return transitionToState(new TerminalState(mapBuilder)).channelForBlob(blob);
        }
    }

    private static abstract class AbstractBlobChannelPairsState implements BlobChannelPairsState {
        @Override
        public void addChannelForBlob(final BulkObject blob, final ByteChannel byteChannel) {
            // Intentionally empty
        }
    }

    private class TerminalState extends AbstractBlobChannelPairsState {
        private TerminalState(final ImmutableMap.Builder<String, BlobChannelPair> mapBuilder) {
            blobChannelPairs = mapBuilder.build();
        }

        @Override
        public boolean containsBlob(final BulkObject blob) {
            return blobChannelPairs.keySet().contains(blob.getUniqueName());
        }

        @Override
        public ByteChannel channelForBlob(final BulkObject blob) {
            final BlobChannelPair blobChannelPair = blobChannelPairs.get(blob.getUniqueName());

            return blobChannelPair == null ? null : blobChannelPair.getChannel();
        }
    }
}
