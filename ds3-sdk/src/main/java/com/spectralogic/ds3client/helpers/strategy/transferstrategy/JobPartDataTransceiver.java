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
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.JobPartTracker;
import com.spectralogic.ds3client.helpers.ObjectPart;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;

import java.io.IOException;

public class JobPartDataTransceiver implements DataTransceiver {
    final ChannelStrategy channelStrategy;
    final BlobStrategy blobStrategy;
    final String bucketName;
    final String jobId;
    final JobPartTracker jobPartTracker;

    public JobPartDataTransceiver(final ChannelStrategy channelStrategy,
                                  final BlobStrategy blobStrategy,
                                  final String bucketName,
                                  final String jobId,
                                  final JobPartTracker jobPartTracker)
    {
        this.channelStrategy = channelStrategy;
        this.blobStrategy = blobStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.jobPartTracker = jobPartTracker;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        final JobPart transferableJobPart = new JobPart(jobPart, channelStrategy.channelForBlob(jobPart.getBulkObject()));
        final BulkObject blob = transferableJobPart.getBulkObject();
        final PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                blob.getName(),
                transferableJobPart.getChannel(),
                jobId,
                blob.getOffset(),
                blob.getLength());
        transferableJobPart.getClient().putObject(putObjectRequest);
        blobStrategy.blobCompleted(blob);
        jobPartTracker.completePart(blob.getName(), new ObjectPart(blob.getOffset(), blob.getLength()));
    }
}
