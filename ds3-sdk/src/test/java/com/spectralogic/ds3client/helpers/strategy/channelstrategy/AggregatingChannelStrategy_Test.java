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
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class AggregatingChannelStrategy_Test {
    @Test
    public void testGettingChannelForOneBlob() throws IOException {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

            final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
            assertNotNull(seekableByteChannel);
            assertTrue(seekableByteChannel.isOpen());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testBlobsWithSameNameAndDifferentOffsetsUseSameChannel() throws IOException {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final BulkObject blob2 = new BulkObject();
            blob2.setName(blobName);
            blob2.setOffset(3);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

            final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
            final SeekableByteChannel seekableByteChannel2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);

            assertEquals(seekableByteChannel, seekableByteChannel2);
        } finally {
            file.delete();
        }
    }

    @Test
    public void testReleasingChannelForOneBlob() throws IOException {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

            final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
            assertTrue(seekableByteChannel.isOpen());
            wrapperChannelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
            assertFalse(seekableByteChannel.isOpen());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testReleasingChannelForSameBlobsWithDifferentOffsets() throws IOException {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final BulkObject blob2 = new BulkObject();
            blob2.setName(blobName);
            blob2.setOffset(3);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

            final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
            final SeekableByteChannel seekableByteChannel2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);

            wrapperChannelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
            assertTrue(seekableByteChannel.isOpen());
            assertTrue(seekableByteChannel2.isOpen());

            wrapperChannelStrategy.releaseChannelForBlob(seekableByteChannel2, blob2);
            assertFalse(seekableByteChannel.isOpen());
            assertFalse(seekableByteChannel2.isOpen());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testReleasingSameBlobOffsetForDifferentBlobs() throws IOException {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final BulkObject blob2 = new BulkObject();
            blob2.setName(blobName);
            blob2.setOffset(3);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

            final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
            final SeekableByteChannel seekableByteChannel2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);

            wrapperChannelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
            wrapperChannelStrategy.releaseChannelForBlob(seekableByteChannel2, blob);

            assertTrue(seekableByteChannel.isOpen());
            assertTrue(seekableByteChannel2.isOpen());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testAggregatingFileChannelStrategyWithFailingStreamAllocation() {
        final String blobName = "Blobby";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", blobName).toString());

        try {
            final BulkObject blob = new BulkObject();
            blob.setName(blobName);
            blob.setOffset(0);

            final BulkObject blob2 = new BulkObject();
            blob2.setName(blobName);
            blob2.setOffset(3);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final ChannelStrategy mockSequentialFileReaderChannelStrategy = new MockSequentialChannelReaderStrategy(channelStrategy, blob2);

            final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(mockSequentialFileReaderChannelStrategy);

            final AtomicInteger numTimesIOExceptionCaught = new AtomicInteger(0);

            try {
                try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                    fileOutputStream.write(expectedText.getBytes());
                }

                wrapperChannelStrategy.acquireChannelForBlob(blob2);
            } catch (final IOException e) {
                numTimesIOExceptionCaught.getAndIncrement();

                try {
                    final SeekableByteChannel seekableByteChannel2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);
                    assertNotNull(seekableByteChannel2);
                    assertTrue(seekableByteChannel2.isOpen());

                    final SeekableByteChannel seekableByteChannel = wrapperChannelStrategy.acquireChannelForBlob(blob);
                    assertNotNull(seekableByteChannel);

                    assertEquals(seekableByteChannel, seekableByteChannel2);
                } catch (final IOException e1) {
                    fail("Allocating channel for blob should not have thrown an exception.");
                }

                assertEquals(1, numTimesIOExceptionCaught.get());
            }
        } finally {
            file.delete();
        }
    }

    private static class MockSequentialChannelReaderStrategy implements ChannelStrategy {
        private final ChannelStrategy wrappedChannelStrategy;
        private final BulkObject blob2;

        private int numTimesGettingChannelForBlob2 = 0;

        private MockSequentialChannelReaderStrategy(final ChannelStrategy wrappedChannelStrategy, final BulkObject blob2) {
            this.wrappedChannelStrategy = wrappedChannelStrategy;
            this.blob2 = blob2;
        }

        @Override
        public SeekableByteChannel acquireChannelForBlob(final BulkObject blob) throws IOException {
            if (blob == blob2 && numTimesGettingChannelForBlob2++ == 0) {
                throw new IOException();
            }

            return wrappedChannelStrategy.acquireChannelForBlob(blob);
        }

        @Override
        public void releaseChannelForBlob(final SeekableByteChannel seekableByteChannel, final BulkObject blob) throws IOException {
            wrappedChannelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
        }
    }

    @Test
    public void testGettingReadChannelForBlobWithNoFile() {
        final String blobName = "Blobby";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);
        blob.setOffset(0);

        final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
        final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

        final AtomicInteger numTimesIOExceptionCaught = new AtomicInteger(0);

        try {
            wrapperChannelStrategy.acquireChannelForBlob(blob);
        } catch (final IOException e) {
            assertTrue(e instanceof NoSuchFileException);
            numTimesIOExceptionCaught.getAndIncrement();
        }

        assertEquals(1, numTimesIOExceptionCaught.get());
    }

    @Test
    public void testGettingReadChannelForBlobWithNoDirectory() {
        final String blobName = "Blobby";

        final BulkObject blob = new BulkObject();
        blob.setName(blobName);
        blob.setOffset(0);

        final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("transfererJustBecauseICan"));
        final ChannelStrategy wrapperChannelStrategy = new AggregatingChannelStrategy(channelStrategy);

        final AtomicInteger numTimesIOExceptionCaught = new AtomicInteger(0);

        try {
            wrapperChannelStrategy.acquireChannelForBlob(blob);
        } catch (final IOException e) {
            assertTrue(e instanceof NoSuchFileException);
            numTimesIOExceptionCaught.getAndIncrement();
        }

        assertEquals(1, numTimesIOExceptionCaught.get());
    }
}
