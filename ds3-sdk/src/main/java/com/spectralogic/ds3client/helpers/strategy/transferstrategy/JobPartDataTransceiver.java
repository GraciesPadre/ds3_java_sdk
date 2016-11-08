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
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.BlobChannelPair;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.io.IOException;
import java.nio.channels.ByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobPartDataTransceiver implements DataTransceiver {
    private static final Logger LOG = LoggerFactory.getLogger(JobPartDataTransceiver.class);

    private final ChannelStrategy channelStrategy;
    private final BlobStrategy blobStrategy;
    private final String bucketName;
    private final String jobId;
    private final JobPartTracker jobPartTracker;
    private final ChecksumFunction checksumFunction;
    private final ChecksumType.Type checksumType;

    public JobPartDataTransceiver(final ChannelStrategy channelStrategy,
                                  final BlobStrategy blobStrategy,
                                  final String bucketName,
                                  final String jobId,
                                  final JobPartTracker jobPartTracker,
                                  final ChecksumFunction checksumFunction,
                                  final ChecksumType.Type checksumType)
    {
        this.channelStrategy = channelStrategy;
        this.blobStrategy = blobStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.jobPartTracker = jobPartTracker;
        this.checksumFunction = checksumFunction;
        this.checksumType = checksumType;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        final BlobChannelPair blobChannelPair = channelStrategy.acquireChannelForBlob(jobPart.getBulkObject());

        jobPart.getClient().putObject(makePutObjectRequest(blobChannelPair, jobPart));

        final BulkObject blob = jobPart.getBulkObject();
        blobStrategy.blobCompleted(blob);
        channelStrategy.releaseChannelForBlob(blobChannelPair);
        jobPartTracker.completePart(blob.getName(), new ObjectPart(blob.getOffset(), blob.getLength()));
    }

    private PutObjectRequest makePutObjectRequest(final BlobChannelPair blobChannelPair, final JobPart jobPart) {
        final BulkObject blob = jobPart.getBulkObject();

        final PutObjectRequest putObjectRequest = new PutObjectRequest(
                bucketName,
                blob.getName(),
                blobChannelPair.getChannel(),
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
                final ByteChannel byteChannel = channelStrategy.acquireChannelForBlob(blob).getChannel();
                checksum = checksumFunction.compute(blob, byteChannel);

                if (checksum != null) {
                    putObjectRequest.withChecksum(ChecksumType.value(checksum), checksumType);
                }
            }  catch (final IOException e) {
                LOG.info("Failure creating channel to calculate checksum.", e);
            }
        }
    }
}
