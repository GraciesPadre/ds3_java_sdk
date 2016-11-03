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

import com.spectralogic.ds3client.models.BulkObject;

import java.nio.channels.ByteChannel;

public class BlobChannelPair {
    private final BulkObject blob;
    private final ByteChannel channel;

    public BlobChannelPair(final BulkObject blob, final ByteChannel channel) {
        this.blob = blob;
        this.channel = channel;
    }

    public BulkObject getBlob() {
        return blob;
    }

    public ByteChannel getChannel() {
        return channel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof BlobChannelPair)) return false;

        final BlobChannelPair that = (BlobChannelPair) o;

        return getBlob() != null ? getBlob().equals(that.getBlob()) : that.getBlob() == null;

    }

    @Override
    public int hashCode() {
        return getBlob() != null ? getBlob().hashCode() : 0;
    }
}
