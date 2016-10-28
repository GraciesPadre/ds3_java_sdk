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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequentialFileWriterChannelStrategy extends AbstractSequentialFileStrategy {
    private final static Logger LOG = LoggerFactory.getLogger(SequentialFileWriterChannelStrategy.class);

    public SequentialFileWriterChannelStrategy(final Path directory) {
        super(directory);
    }

    @Override
    protected void populateBlobChannelPairs(final BulkObjectList blobs,
                                            final ChannelAllocationFailureHandler channelAllocationFailureHandler,
                                            final BlobChannelPairs blobChannelPairs)
    {
        for (final BulkObject blob : blobs.getObjects()) {
            try {
                if ( ! blobChannelPairs.containsBlob(blob)) {
                    Files.createDirectories(getDirectory());

                    final Path filePath = Paths.get(getDirectory().toString(), blob.getName());
                    createFile(filePath);

                    final FileOutputStream fileOutputStream = new FileOutputStream(filePath.toFile());
                    final ByteChannel byteChannel = fileOutputStream.getChannel();

                    blobChannelPairs.addChannelForBlob(blob, byteChannel);
                }
            } catch (final Throwable t) {
                if (channelAllocationFailureHandler != null) {
                    channelAllocationFailureHandler.onChannelAllocationFailure(blob.getName(), t);
                }
            }
        }
    }

    private void createFile(final Path filePath) {
        try {
            Files.createFile(filePath);
        } catch (final IOException e) {
            LOG.info("File already exists", e);
        }
    }
}
