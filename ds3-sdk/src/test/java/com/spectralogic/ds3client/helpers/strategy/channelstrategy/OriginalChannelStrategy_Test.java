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

import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.BulkObject;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;

public class OriginalChannelStrategy_Test {
    @Test
    public void testGettingChannelForABlobAndNoRanges() throws IOException {
        final String directoryName = ".";
        final Path directory = Paths.get(directoryName);
        final String blobName = "aBlob";
        final Path filePath = Paths.get(directoryName, blobName);

        final Path actualFilePath = Files.createFile(filePath);

        try {
            final BulkObject blob = Mockito.mock(BulkObject.class);
            Mockito.when(blob.getName()).thenReturn(blobName);

            final ChannelStrategy channelStrategy = new OriginalChannelStrategy(new FileObjectPutter(directory),
                    null, new NullChannelPreparable());

            final SeekableByteChannel seekableByteChannel = channelStrategy.acquireChannelForBlob(blob);

            assertNotNull(seekableByteChannel);
        } finally {
            Files.delete(actualFilePath);
        }
    }

    @Test
    public void testThatClosingChannelDoesNotThrow() throws IOException {
        final String directoryName = ".";
        final Path directory = Paths.get(directoryName);
        final String blobName = "aBlob";
        final Path filePath = Paths.get(directoryName, blobName);

        final Path actualFilePath = Files.createFile(filePath);

        try {
            final BulkObject blob = Mockito.mock(BulkObject.class);
            Mockito.when(blob.getName()).thenReturn(blobName);

            final ChannelStrategy channelStrategy = new OriginalChannelStrategy(new FileObjectPutter(directory),
                    null, new NullChannelPreparable());

            final SeekableByteChannel seekableByteChannel = channelStrategy.acquireChannelForBlob(blob);

            assertNotNull(seekableByteChannel);

            channelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
        } finally {
            Files.delete(actualFilePath);
        }
    }
}
