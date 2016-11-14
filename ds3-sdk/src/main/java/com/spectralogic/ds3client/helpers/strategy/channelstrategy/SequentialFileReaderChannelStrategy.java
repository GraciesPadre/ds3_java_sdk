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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.filterChunks;
import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.makeResettableInputStream;
import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.resolveForSymbolic;

public class SequentialFileReaderChannelStrategy extends AbstractChannelStrategy {
    private final Path directory;

    public SequentialFileReaderChannelStrategy(final Path directory) {
        this.directory = directory;
    }

    @Override
    public BlobChannelStreamQuad acquireChannelForBlob(final BulkObject blob) throws IOException {
        synchronized (getLock()) {
            final Path path = directory.resolve(blob.getName());

            final FileChannel channel = FileChannel.open(resolveForSymbolic(path), StandardOpenOption.READ);
            channel.position(blob.getOffset());

            final InputStream inputStream = makeResettableInputStream(Channels.newInputStream(channel));

            return new BlobChannelStreamQuad.Builder(blob)
                    .withChannel(channel)
                    .withInputStream(inputStream)
                    .build();
        }
    }
}
