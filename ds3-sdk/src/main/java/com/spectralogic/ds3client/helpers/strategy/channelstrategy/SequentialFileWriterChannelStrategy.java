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

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequentialFileWriterChannelStrategy extends AbstractChannelStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialFileWriterChannelStrategy.class);

    private final Path directory;

    public SequentialFileWriterChannelStrategy(final Path directory) {
        this.directory = directory;
    }

    @Override
    public BlobChannelPair acquireChannelForBlob(final BulkObject blob) throws IOException {
        Files.createDirectories(directory);

        final Path filePath = Paths.get(directory.toString(), blob.getName());
        createFile(filePath);

        final ByteChannel channel =  FileChannel.open(filePath,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);

        return new BlobChannelPair(blob, channel);
    }

    private void createFile(final Path filePath) {
        try {
            Files.createFile(filePath);
        } catch (final IOException e) {
            LOG.info("File already exists", e);
        }
    }
}
