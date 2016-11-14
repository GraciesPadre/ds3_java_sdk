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
import java.util.HashMap;
import java.util.Map;

public class SequentialAggregatingChannelStrategy implements ChannelStrategy {
    private final Object lock = new Object();

    private final SetMultimap<String, Long> blobNameOffsetMap = HashMultimap.create();
    private final Map<String, BlobChannelStreamQuad> blobNameChannelMap = new HashMap<>();

    private final ChannelStrategy channelStrategyDelegate;

    public SequentialAggregatingChannelStrategy(final ChannelStrategy channelStrategy) {
        channelStrategyDelegate = channelStrategy;
    }

    @Override
    public BlobChannelStreamQuad acquireChannelForBlob(final BulkObject blob) throws IOException {
        synchronized (lock) {
            final String blobName = blob.getName();

            blobNameOffsetMap.put(blobName, blob.getOffset());

            final BlobChannelStreamQuad blobChannelStreamQuad = blobNameChannelMap.get(blobName);

            if (blobChannelStreamQuad != null) {
                return new BlobChannelStreamQuad.Builder(blob)
                        .withChannel(blobChannelStreamQuad.getChannel())
                        .withInputStream(blobChannelStreamQuad.getInputStream())
                        .build();
            }

            return makeNewBlobChanneQuad(blob);
        }
    }

    private BlobChannelStreamQuad makeNewBlobChanneQuad(final BulkObject blob) throws IOException {
        final BlobChannelStreamQuad blobChannelStreamQuad = channelStrategyDelegate.acquireChannelForBlob(blob);

        blobNameChannelMap.put(blob.getName(), blobChannelStreamQuad);

        return blobChannelStreamQuad;
    }

    @Override
    public BlobChannelStreamQuad releaseChannelForBlob(final BlobChannelStreamQuad blobChannelStreamQuad) throws IOException {
        synchronized (lock) {
            final String blobName = blobChannelStreamQuad.getBlob().getName();

            blobNameOffsetMap.remove(blobName, blobChannelStreamQuad.getBlob().getOffset());

            if (blobNameOffsetMap.get(blobName).size() == 0) {
                blobNameChannelMap.remove(blobName);
                return channelStrategyDelegate.releaseChannelForBlob(blobChannelStreamQuad);
            }

            return blobChannelStreamQuad;
        }
    }
}
