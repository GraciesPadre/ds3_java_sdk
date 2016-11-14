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
import java.nio.file.Paths;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SequentialAggregatingChannelStrategy_Test {
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
            final ChannelStrategy wrapperChannelStrategy = new SequentialAggregatingChannelStrategy(channelStrategy);

            final BlobChannelStreamQuad blobChannelStreamQuad = wrapperChannelStrategy.acquireChannelForBlob(blob);

            assertNotNull(blobChannelStreamQuad.getBlob());
            assertNotNull(blobChannelStreamQuad.getChannel());
            assertNotNull(blobChannelStreamQuad.getInputStream());
            assertNull(blobChannelStreamQuad.getOutputStream());
            assertEquals(0, blobChannelStreamQuad.getBlob().getOffset());
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
            final ChannelStrategy wrapperChannelStrategy = new SequentialAggregatingChannelStrategy(channelStrategy);

            final BlobChannelStreamQuad blobChannelStreamQuad = wrapperChannelStrategy.acquireChannelForBlob(blob);

            assertNotNull(blobChannelStreamQuad.getBlob());
            assertNotNull(blobChannelStreamQuad.getChannel());
            assertNotNull(blobChannelStreamQuad.getInputStream());
            assertNull(blobChannelStreamQuad.getOutputStream());
            assertEquals(0, blobChannelStreamQuad.getBlob().getOffset());

            final BlobChannelStreamQuad blobChannelStreamQuad2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);

            assertNotNull(blobChannelStreamQuad2.getBlob());
            assertNotNull(blobChannelStreamQuad2.getChannel());
            assertNotNull(blobChannelStreamQuad2.getInputStream());
            assertNull(blobChannelStreamQuad2.getOutputStream());
            assertEquals(3, blobChannelStreamQuad2.getBlob().getOffset());

            assertEquals(blobChannelStreamQuad.getChannel(), blobChannelStreamQuad2.getChannel());
            assertEquals(blobChannelStreamQuad.getInputStream(), blobChannelStreamQuad2.getInputStream());
            assertEquals(blobChannelStreamQuad.getOutputStream(), blobChannelStreamQuad2.getOutputStream());
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
            final ChannelStrategy wrapperChannelStrategy = new SequentialAggregatingChannelStrategy(channelStrategy);

            final BlobChannelStreamQuad blobChannelStreamQuad = wrapperChannelStrategy.acquireChannelForBlob(blob);

            assertNotNull(blobChannelStreamQuad.getBlob());
            assertNotNull(blobChannelStreamQuad.getChannel());
            assertNotNull(blobChannelStreamQuad.getInputStream());
            assertNull(blobChannelStreamQuad.getOutputStream());
            assertEquals(0, blobChannelStreamQuad.getBlob().getOffset());

            final BlobChannelStreamQuad releasedBlobChannelQuad = wrapperChannelStrategy.releaseChannelForBlob(blobChannelStreamQuad);

            assertNull(releasedBlobChannelQuad.getChannel());
            assertNull(releasedBlobChannelQuad.getInputStream());
            assertNull(releasedBlobChannelQuad.getOutputStream());
            assertEquals(0, releasedBlobChannelQuad.getBlob().getOffset());
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
            final ChannelStrategy wrapperChannelStrategy = new SequentialAggregatingChannelStrategy(channelStrategy);

            final BlobChannelStreamQuad blobChannelStreamQuad = wrapperChannelStrategy.acquireChannelForBlob(blob);

            assertNotNull(blobChannelStreamQuad.getBlob());
            assertNotNull(blobChannelStreamQuad.getChannel());
            assertNotNull(blobChannelStreamQuad.getInputStream());
            assertNull(blobChannelStreamQuad.getOutputStream());
            assertEquals(0, blobChannelStreamQuad.getBlob().getOffset());

            final BlobChannelStreamQuad blobChannelStreamQuad2 = wrapperChannelStrategy.acquireChannelForBlob(blob2);

            assertNotNull(blobChannelStreamQuad2.getBlob());
            assertNotNull(blobChannelStreamQuad2.getChannel());
            assertNotNull(blobChannelStreamQuad2.getInputStream());
            assertNull(blobChannelStreamQuad2.getOutputStream());
            assertEquals(3, blobChannelStreamQuad2.getBlob().getOffset());

            assertEquals(blobChannelStreamQuad.getChannel(), blobChannelStreamQuad2.getChannel());
            assertEquals(blobChannelStreamQuad.getInputStream(), blobChannelStreamQuad2.getInputStream());
            assertEquals(blobChannelStreamQuad.getOutputStream(), blobChannelStreamQuad2.getOutputStream());

            final BlobChannelStreamQuad releasedBlobChannelQuad = wrapperChannelStrategy.releaseChannelForBlob(blobChannelStreamQuad);

            assertEquals(blobChannelStreamQuad.getChannel(), releasedBlobChannelQuad.getChannel());
            assertEquals(blobChannelStreamQuad.getInputStream(), releasedBlobChannelQuad.getInputStream());
            assertEquals(blobChannelStreamQuad.getOutputStream(), releasedBlobChannelQuad.getOutputStream());
            assertEquals(0, releasedBlobChannelQuad.getBlob().getOffset());

            final BlobChannelStreamQuad releasedBlobChannelQuad2 = wrapperChannelStrategy.releaseChannelForBlob(blobChannelStreamQuad2);

            assertEquals(blobChannelStreamQuad2.getBlob(), releasedBlobChannelQuad2.getBlob());
            assertNull(releasedBlobChannelQuad2.getInputStream());
            assertNull(releasedBlobChannelQuad2.getOutputStream());
            assertEquals(3, releasedBlobChannelQuad2.getBlob().getOffset());
        } finally {
            file.delete();
        }
    }
}
