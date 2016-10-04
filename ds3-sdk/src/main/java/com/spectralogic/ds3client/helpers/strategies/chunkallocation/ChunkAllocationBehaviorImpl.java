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

package com.spectralogic.ds3client.helpers.strategies.chunkallocation;

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.AllocateJobChunkSpectraS3Request;
import com.spectralogic.ds3client.commands.spectrads3.AllocateJobChunkSpectraS3Response;
import com.spectralogic.ds3client.models.Objects;

import java.io.IOException;

public class ChunkAllocationBehaviorImpl implements ChunkAllocationBehavior {
    private final Ds3Client ds3Client;
    private final RetryBehavior retryBehavior;
    private final DelayBehavior delayBehavior;

    public ChunkAllocationBehaviorImpl(final Ds3Client ds3Client,
                                       final RetryBehavior retryBehavior,
                                       final DelayBehavior delayBehavior) {
        Preconditions.checkNotNull(ds3Client, "ds3Client must not be null");
        Preconditions.checkNotNull(retryBehavior, "retryBehavior must not be null");
        Preconditions.checkNotNull(delayBehavior, "delayBehavior must not be null");

        this.ds3Client = ds3Client;
        this.retryBehavior = retryBehavior;
        this.delayBehavior = delayBehavior;
    }

    @Override
    public Objects allocateDestinationChunks(final Objects sourceChunks) throws IOException {
        final AllocateJobChunkSpectraS3Response response = ds3Client.allocateJobChunkSpectraS3(new AllocateJobChunkSpectraS3Request(sourceChunks.getChunkId().toString()));

        switch (response.getStatus()) {
            case ALLOCATED:
                retryBehavior.reset();
                return response.getObjectsResult();
            case RETRYLATER:
                retryBehavior.processIteration();
                delayBehavior.delay(response.getRetryAfterSeconds());
                return null;
            default:
                assert false : "This line of code should be impossible to hit.";
                return null;
        }
    }
}
