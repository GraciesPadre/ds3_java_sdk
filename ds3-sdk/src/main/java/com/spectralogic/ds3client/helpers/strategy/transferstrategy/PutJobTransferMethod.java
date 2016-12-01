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

package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.helpers.ChecksumFunction;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.SeekableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutJobTransferMethod implements TransferMethod {
    private static final Logger LOG = LoggerFactory.getLogger(PutJobTransferMethod.class);

    private final TransferStrategy transferStrategy;
    private final ChannelStrategy channelStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventDispatcher eventDispatcher;
    private final ChecksumFunction checksumFunction;
    private final ChecksumType.Type checksumType;

    public PutJobTransferMethod(final TransferStrategy transferStrategy,
                                final ChannelStrategy channelStrategy,
                                final String bucketName,
                                final String jobId,
                                final EventDispatcher eventDispatcher,
                                final ChecksumFunction checksumFunction,
                                final ChecksumType.Type checksumType)
    {
        this.transferStrategy = transferStrategy;
        this.channelStrategy = channelStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventDispatcher = eventDispatcher;
        this.checksumFunction = checksumFunction;
        this.checksumType = checksumType;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        final SeekableByteChannel seekableByteChannel = channelStrategy.acquireChannelForBlob(jobPart.getBulkObject());

        jobPart.getClient().putObject(makePutObjectRequest(seekableByteChannel, jobPart));

        final BulkObject blob = jobPart.getBulkObject();
        channelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
        eventDispatcher.emitBlobTransferredEvent(blob);
        eventDispatcher.emitDataTransferredEvent(blob);
    }

    private PutObjectRequest makePutObjectRequest(final SeekableByteChannel seekableByteChannel, final JobPart jobPart) {
        final BulkObject blob = jobPart.getBulkObject();

        final PutObjectRequest putObjectRequest = new PutObjectRequest(
                bucketName,
                blob.getName(),
                seekableByteChannel,
                jobId,
                blob.getOffset(),
                blob.getLength());

        addCheckSum(blob, putObjectRequest);

        return putObjectRequest;
    }

    private void addCheckSum(final BulkObject blob,
                             final PutObjectRequest putObjectRequest)
    {
        if (checksumFunction != null) {
            final String checksum;

            try {
                final ByteChannel byteChannel = channelStrategy.acquireChannelForBlob(blob);
                checksum = checksumFunction.compute(blob, byteChannel);

                if (checksum != null) {
                    putObjectRequest.withChecksum(ChecksumType.value(checksum), checksumType);
                    eventDispatcher.emitChecksumEvent(blob, checksumType, checksum);
                }
            }  catch (final IOException e) {
                // TODO Emit a failure event here
                LOG.info("Failure creating channel to calculate checksum.", e);
            }
        }
    }
}
