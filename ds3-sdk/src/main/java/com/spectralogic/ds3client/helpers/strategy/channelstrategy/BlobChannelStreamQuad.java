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
import com.spectralogic.ds3client.models.BulkObject;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ByteChannel;

public class BlobChannelStreamQuad {
    private final BulkObject blob;
    private final ByteChannel channel;
    private final InputStream inputStream;
    private final OutputStream outputStream;

    private BlobChannelStreamQuad(
            final BulkObject blob,
            final ByteChannel channel,
            final InputStream inputStream,
            final OutputStream outputStream)
    {
        this.blob = blob;
        this.channel = channel;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    public BulkObject getBlob() {
        return blob;
    }

    public ByteChannel getChannel() {
        return channel;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof BlobChannelStreamQuad)) return false;

        final BlobChannelStreamQuad that = (BlobChannelStreamQuad) o;

        if (!getBlob().equals(that.getBlob())) return false;
        if (!getChannel().equals(that.getChannel())) return false;
        if (getInputStream() != null ? !getInputStream().equals(that.getInputStream()) : that.getInputStream() != null)
            return false;
        return getOutputStream() != null ? getOutputStream().equals(that.getOutputStream()) : that.getOutputStream() == null;

    }

    @Override
    public int hashCode() {
        int result = getBlob().hashCode();
        result = 31 * result + getChannel().hashCode();
        result = 31 * result + (getInputStream() != null ? getInputStream().hashCode() : 0);
        result = 31 * result + (getOutputStream() != null ? getOutputStream().hashCode() : 0);
        return result;
    }

    public static class Builder {
        private final BulkObject blob;
        private ByteChannel channel;
        private InputStream inputStream;
        private OutputStream outputStream;

        public Builder(final BulkObject blob) {
            this.blob = blob;
        }

        public Builder withChannel(final ByteChannel channel) {
            this.channel = channel;
            return this;
        }

        public Builder withInputStream(final InputStream inputStream) {
            this.inputStream = inputStream;
            return this;
        }

        public Builder withOutputStream(final OutputStream outputStream) {
            this.outputStream = outputStream;
            return this;
        }

        public BlobChannelStreamQuad build() {
            Preconditions.checkNotNull(blob, "blob may not be null.");

            return new BlobChannelStreamQuad(blob, channel, inputStream, outputStream);
        }
    }
}
