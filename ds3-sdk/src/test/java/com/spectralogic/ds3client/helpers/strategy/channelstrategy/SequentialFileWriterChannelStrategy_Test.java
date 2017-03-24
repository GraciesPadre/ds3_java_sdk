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

import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.models.BulkObject;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class SequentialFileWriterChannelStrategy_Test {
    @Test
    public void testThatBlobsReferringToExistingFileTruncate() {
        final String channelName = "Gracie.txt";
        final String expectedText = "Gracie";

        final File file = new File(Paths.get(".", channelName).toString());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write("0123456789".getBytes());
            }

            final Ds3ClientHelpers.ObjectChannelBuilder objectChannelBuilder = new FileObjectGetter(Paths.get("."));
            final ChannelStrategy channelStrategy = new SequentialFileWriterChannelStrategy(objectChannelBuilder);
            final ChannelStrategy sequentialFileWriterStrategy = new SequentialChannelStrategy(channelStrategy, objectChannelBuilder, new TruncatingChannelPreparable());


            final BulkObject blob = new BulkObject();
            blob.setName(channelName);

            final ByteChannel byteChannel = sequentialFileWriterStrategy.acquireChannelForBlob(blob);

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
}
