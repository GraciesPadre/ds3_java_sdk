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

            final ByteChannel byteChannel = channelStrategy.acquireChannelForBlob(blob);

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

    @Test(expected = IOException.class)
    public void testThatInvalidFileFiresFailureHandler() throws IOException {
        final String channelName = "Gracie.txt";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            final ChannelStrategy channelStrategy = new SequentialFileReaderChannelStrategy(Paths.get("."));

            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            channelStrategy.acquireChannelForBlob(blob);
        } finally {
            file.delete();
        }
    }
}
