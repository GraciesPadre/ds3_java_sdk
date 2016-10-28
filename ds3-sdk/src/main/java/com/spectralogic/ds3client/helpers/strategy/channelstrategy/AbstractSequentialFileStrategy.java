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
import com.spectralogic.ds3client.models.BulkObjectList;

import java.nio.file.Path;

public abstract class AbstractSequentialFileStrategy implements ChannelStrategy {
    private final Path directory;
    private BlobCompletionHandler blobCompletionHandler = null;

    public AbstractSequentialFileStrategy(final Path directory) {
        this.directory = directory;
    }

    protected Path getDirectory() {
        return directory;
    }

    @Override
    public BlobChannelPairs channelsForBlobs(final BulkObjectList blobs,
                                             final ChannelAllocationFailureHandler channelAllocationFailureHandler)
    {
        final BlobChannelPairs blobChannelPairs = new BlobChannelPairs();

        populateBlobChannelPairs(blobs, channelAllocationFailureHandler, blobChannelPairs);

        return blobChannelPairs;
    }

    protected abstract void populateBlobChannelPairs(final BulkObjectList blobs,
                                            final ChannelAllocationFailureHandler channelAllocationFailureHandler,
                                            final BlobChannelPairs blobChannelPairs);


    @Override
    public void withBlobCompletionHandler(final BlobCompletionHandler blobCompletionHandler) {
        this.blobCompletionHandler = blobCompletionHandler;
    }

    @Override
    public void completeBlob(final BulkObject blob) {
        if (blobCompletionHandler != null) {
            blobCompletionHandler.onBlobComplete(blob);
        }
    }
}
