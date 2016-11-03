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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class SequentialFileWriterChannelStrategy_Test {
    /*
    @Test
    public void testThatMultipleBlobsWithTheSameBucketAndNameMapToTheSameChannel() {
        final String bucketName = "bucket";
        final String blobName = "theBlobThatAteGunbarrel";

        final BulkObject blob1 = new BulkObject();
        blob1.setBucket(bucketName);
        blob1.setName(blobName);
        blob1.setId(UUID.randomUUID());
        blob1.setLength(100);
        blob1.setOffset(0);

        final BulkObject blob2 = new BulkObject();
        blob2.setBucket(bucketName);
        blob2.setName(blobName);
        blob2.setId(UUID.randomUUID());
        blob2.setLength(200);
        blob2.setOffset(50);

        try {
            final BulkObjectList blobs = new BulkObjectList();
            blobs.setObjects(Arrays.asList(new BulkObject[]{blob1, blob2}));

            final SequentialFileWriterChannelStrategy sequentialFileWriterChannelStrategy = new SequentialFileWriterChannelStrategy(Paths.get("."));
            final BlobChannelPairs blobChannelPairs = sequentialFileWriterChannelStrategy.channelsForBlobs(blobs, new ChannelAllocationFailureHandler() {
                @Override
                public void onChannelAllocationFailure(final String channelName, final Throwable causalException) {
                    fail("Error creating channel: " + channelName + ", error: " + causalException.getMessage());
                }
            });

            final ByteChannel channel1 = blobChannelPairs.acquireChannelForBlob(blob1);
            assertNotNull(channel1);

            final ByteChannel channel2 = blobChannelPairs.acquireChannelForBlob(blob2);
            assertNotNull(channel2);

            assertTrue(channel1.equals(channel2));
        } finally {
            Paths.get(".", blob1.getName()).toFile().delete();
        }
    }

    @Test
    public void testThatBlobsReferringToExistingFileTruncate() {
        final String channelName = "Gracie.txt";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write("0123456789".getBytes());
            }

            final ChannelStrategy channelStrategy = new SequentialFileWriterChannelStrategy(Paths.get("."));

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

            final ByteChannel byteChannel = blobChannelPairs.acquireChannelForBlob(blob);

            try {
                final ByteBuffer writeBuffer = ByteBuffer.allocate(32);
                writeBuffer.put(expectedText.getBytes());
                writeBuffer.flip();
                byteChannel.write(writeBuffer);

                try (final FileInputStream fileInputStream = new FileInputStream(file)) {
                    final byte[] bytes = new byte[32];
                    final int numBytesRead = fileInputStream.read(bytes);
                    final byte[] relevantBytes = Arrays.copyOfRange(bytes, 0, numBytesRead);
                    final String got = new String(relevantBytes);
                    assertEquals(expectedText, got);
                }
            } catch (final IOException e) {
                fail("Failure with channel: " + channelName + " with exception: " + e.getMessage());
            }
        } catch (final IOException e) {
            fail("Failure with channel: " + channelName + " with exception: " + e.getMessage());
        } finally {
            file.delete();
        }
    }
    */
}
