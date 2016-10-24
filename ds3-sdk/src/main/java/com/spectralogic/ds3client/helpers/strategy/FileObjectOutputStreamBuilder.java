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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

public class FileObjectOutputStreamBuilder implements ObjectOutputStreamBuilder {
    private final Path directory;

    public FileObjectOutputStreamBuilder(final Path directory) {
        this.directory = directory;
    }

    @Override
    public OutputStream buildObjectStream(final String objectName) throws IOException {
        final Path objectPath = directory.resolve(objectName);
        return new FileOutputStream(StrategyUtils.resolveForSymbolicLink(objectPath).toFile());
    }
}

