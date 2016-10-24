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

package com.spectralogic.ds3client.helpers.strategy;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class StreamStrategy_Test {
    @Test
    public void testDs3ObjectForExistingFileReturnsValidStream() throws IOException {
        final String fileName = "Gracie.txt";

        final File file = new File(fileName);

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                IOUtils.write("Gracie", fileOutputStream);
            }

            try (final InputStream inputStream = new FileObjectInputStreamBuilder(Paths.get(System.getProperty("user.dir"))).buildObjectStream(fileName)) {
                assertNotNull(inputStream);
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testDs3ObjectForNonExistentFileThrows() throws IOException {
        final String fileName = "Gracie.txt";
        final File file = new File(fileName);

        boolean caughtIOExeption = false;

        try {
            new FileObjectInputStreamBuilder(Paths.get(System.getProperty("user.dir"))).buildObjectStream(fileName);
        } catch (final IOException e) {
            caughtIOExeption = true;
        }

        assertTrue(caughtIOExeption);
    }

    @Test
    public void testDs3ObjectExistingUrlReturnsValidStream() throws IOException {
        try (final InputStream inputStream = new UrlObjectInputStreamBuilder().buildObjectStream("http://www.google.com")) {
            assertNotNull(inputStream);
        }
    }

    @Test
    public void testDs3ObjectExistingUrlThrows() {
        boolean caughtIOException = false;

        try (final InputStream inputStream = new UrlObjectInputStreamBuilder().buildObjectStream("Giggle pants")) {
            assertNotNull(inputStream);
        } catch (final IOException e) {
            caughtIOException = true;
        }

        assertTrue(caughtIOException);
    }
}
