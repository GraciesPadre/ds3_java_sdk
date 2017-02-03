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

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ConsumerQueueThrottlingStrategy<QueueContentType> extends AbstractThrottlingStrategy<QueueContentType> {
    private final ConsumerRunnable<QueueContentType> consumerRunnable;

    public ConsumerQueueThrottlingStrategy(final WorkQueue<QueueContentType> workQueue,
                                           final ExecutorService executorService,
                                           final long numItemsToProcess,
                                           final ConsumerRunnable<QueueContentType> consumerRunnable,
                                           final TransferCompleteHandler transferCompleteHandler)
    {
        super(workQueue, executorService, numItemsToProcess, transferCompleteHandler);
        this.consumerRunnable = consumerRunnable;
    }

    @Override
    public void run() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                runInExecutor();
            }
        }).start();
    }

    @Override
    protected void perform() throws IOException {
        consumerRunnable.run(dequeueItem());
    }
}
