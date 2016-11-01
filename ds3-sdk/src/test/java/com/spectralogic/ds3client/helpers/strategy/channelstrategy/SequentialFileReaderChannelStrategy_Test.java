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
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SequentialFileReaderChannelStrategy_Test {
    /*
    @Test
    public void testThatSequentialFileReaderIsOpenWhenCreated() {
        final String channelName = "Gracie.txt";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob}));

            final BlobChannelPairs blobChannelPairs = channelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    fail("Failed allocating channel: " + channelName + " with exception: " + causalException.getMessage());
                }
            });

            final ByteChannel byteChannel = blobChannelPairs.channelForBlob(blob);

            assertTrue(byteChannel.isOpen());

            try {
                final ByteBuffer readBuffer = ByteBuffer.allocate(32);
                byteChannel.read(readBuffer);
                readBuffer.flip();
                final String got = new String(readBuffer.array(),
                        readBuffer.arrayOffset() + readBuffer.position(),
                        readBuffer.remaining());
                assertEquals(expectedText, got);
            } catch (final IOException e) {
                fail("Failed reading from channel: " + channelName + " with exception: " + e.getMessage());
            }
        } catch (final IOException e) {
            fail("Failed writing to channel: " + channelName + " with exception: " + e.getMessage());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testThatBlobCompletionFiresHandler() {
        final String channelName = "Gracie.txt";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob}));

            final BlobChannelPairs blobChannelPairs = channelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    fail("Failed allocating channel: " + channelName + " with exception: " + causalException.getMessage());
                }
            });

            final AtomicBoolean channelClosed = new AtomicBoolean(false);

            channelStrategy.withBlobCompletionHandler(new BlobCompletionHandler() {
                @Override
                public void onBlobComplete(final BulkObject blob) {
                    final ByteChannel byteChannel = blobChannelPairs.channelForBlob(blob);
                    try {
                        byteChannel.close();
                        channelClosed.set(true);
                    } catch (final IOException e) {
                        fail("Failed closing channel.");
                    }
                }
            });

            channelStrategy.completeBlob(blob);

            assertTrue(channelClosed.get());
        } catch (final IOException e) {
            fail("Failed writing to channel: " + channelName + " with exception: " + e.getMessage());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testThatNullCompletionHandlerDoesNotCrash() {
        final String channelName = "Gracie.txt";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(expectedText.getBytes());
            }

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob}));

            channelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    fail("Failed allocating channel: " + channelName + " with exception: " + causalException.getMessage());
                }
            });

            channelStrategy.completeBlob(blob);
        } catch (final IOException e) {
            fail("Failed writing to channel: " + channelName + " with exception: " + e.getMessage());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testThatInvalidFileFiresFailureHandler() {
        final String channelName = "Gracie.txt";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob}));

            final AtomicBoolean allocationFailed = new AtomicBoolean(false);

            channelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    allocationFailed.set(true);
                }
            });

            assertTrue(allocationFailed.get());
        } finally {
            file.delete();
        }
    }

    @Test
    public void testThatNullAllocationFailureHandlerDoesNotCrash() {
        final String channelName = "Gracie.txt";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob}));

            try {
                final ChannelAllocationFailureHandler nullChannelAllocationFailureHandler = null;
                channelStrategy.channelsForBlobs(blobs, nullChannelAllocationFailureHandler);
            } catch (final Throwable t) {
                fail("There is a bug with how we're referencing channel failure notification handlers: " + t.getMessage());
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testThatWeAllocateChannelsForValidFilesWhenThereAreSomeInvalidOnes() {
        final String[] channelNames = new String[] { "Gracie.txt", "Gracie2.txt" };
        final String expectedText = "Gracie";

        try {
            for (final String channelName : channelNames) {
                final File file = new File(Paths.get(".", channelName).toString());

                try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                    fileOutputStream.write(expectedText.getBytes());
                }
            }

            final BulkObject goodBlob1 = new BulkObject();
            goodBlob1.setName(channelNames[0]);

            final BulkObject badBlob = new BulkObject();
            final String badChannelName = "Gack";
            badBlob.setName(badChannelName);

            final BulkObject goodBlob2 = new BulkObject();
            goodBlob2.setName(channelNames[1]);

            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{ goodBlob1, badBlob, goodBlob2 }));

            final AtomicInteger numChannelFailures = new AtomicInteger(0);

            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));
            final BlobChannelPairs blobChannelPairs = channelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    assertEquals(channelName, badChannelName);
                    numChannelFailures.getAndIncrement();
                }
            });

            assertEquals(1, numChannelFailures.get());

            ByteChannel channel = blobChannelPairs.channelForBlob(blobs.getObjects().get(0));
            assertNotNull(channel);

            channel = blobChannelPairs.channelForBlob(blobs.getObjects().get(1));
            assertNull(channel);

            channel = blobChannelPairs.channelForBlob(blobs.getObjects().get(2));
            assertNotNull(channel);
        } catch (final Throwable t) {
            fail("Failure creating test files: " + t.getMessage());
        } finally {
            for (final String channelName : channelNames) {
                final File file = new File(Paths.get(".", channelName).toString());
                file.delete();
            }
        }

    }
    */
}
