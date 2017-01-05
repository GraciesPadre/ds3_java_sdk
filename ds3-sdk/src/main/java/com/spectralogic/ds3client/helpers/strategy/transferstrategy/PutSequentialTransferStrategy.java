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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.strategy.blobstrategy.BlobStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class PutSequentialTransferStrategy implements TransferStrategy {
    private final BlobStrategy blobStrategy;
    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    private TransferMethod transferMethod;

    public PutSequentialTransferStrategy(final BlobStrategy blobStrategy) {
        this.blobStrategy = blobStrategy;
    }

    public PutSequentialTransferStrategy withTransferMethod(final TransferMethod transferMethod) {
        this.transferMethod = transferMethod;
        return this;
    }

    @Override
    public void transfer() throws IOException, InterruptedException {
        final List<ListenableFuture<Void>> transferTasks = new ArrayList<>();

        final Iterable<JobPart> workQueue = blobStrategy.getWork();

        for (final JobPart jobPart : workQueue) {
            transferTasks.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    transferMethod.transferJobPart(jobPart);
                    return null;
                }
            }));
        }

        runTransferTasks(ImmutableList.copyOf(transferTasks));
    }

    private void runTransferTasks(final Iterable<ListenableFuture<Void>> transferTasks) throws IOException {
        try {
            Futures.allAsList(transferTasks).get();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            // The future throws a wrapper exception, but we want don't want to expose that this was implemented with futures.
            final Throwable cause = e.getCause();

            // Throw each of the advertised thrown exceptions.
            if (cause instanceof IOException) {
                throw (IOException)cause;
            }

            // The rest we don't know about, so we'll just forward them.
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}
