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

public class BlobChannelPairs {
    private ImmutableMap.Builder<BulkObject, ByteChannel> mapBuilder;
    private ImmutableMap<BulkObject, ByteChannel> blobChannelPairs;

    public void prepare() {
        mapBuilder = new ImmutableMap.Builder<BulkObject, ByteChannel>();
    }

    public void add(final BulkObject blob, final ByteChannel byteChannel) {
        mapBuilder.put(blob, byteChannel);
    }

    public void commit() {
        blobChannelPairs = mapBuilder.build();
    }

    public ByteChannel channelForBlob(final BulkObject blob) {
        return blobChannelPairs.get(blob);
    }
}
