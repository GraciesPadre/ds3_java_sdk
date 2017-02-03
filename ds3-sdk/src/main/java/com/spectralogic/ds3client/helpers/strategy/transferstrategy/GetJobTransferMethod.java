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

import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class GetJobTransferMethod implements TransferMethod {
    private final ChannelStrategy channelStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventDispatcher eventDispatcher;

    public GetJobTransferMethod(final ChannelStrategy channelStrategy,
                                final String bucketName,
                                final String jobId,
                                final EventDispatcher eventDispatcher)
    {
        this.channelStrategy = channelStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        final SeekableByteChannel seekableByteChannel = channelStrategy.acquireChannelForBlob(jobPart.getBulkObject());

        jobPart.getClient().getObject(makeGetObjectRequest(seekableByteChannel, jobPart));

        final BulkObject blob = jobPart.getBulkObject();
        channelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
        eventDispatcher.emitBlobTransferredEvent(blob);
        eventDispatcher.emitDataTransferredEvent(blob);
    }

    private GetObjectRequest makeGetObjectRequest(final SeekableByteChannel seekableByteChannel, final JobPart jobPart) {
        final BulkObject blob = jobPart.getBulkObject();

        final GetObjectRequest getObjectRequest = new GetObjectRequest(
                bucketName,
                blob.getName(),
                seekableByteChannel,
                jobId,
                blob.getOffset());

        return getObjectRequest;
    }
}
