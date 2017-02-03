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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public abstract class AbstractThrottlingStrategy<QueueContentType> implements ThrottlingStrategy {
    private final WorkQueue<QueueContentType> workQueue;
    private final long numItemsToProcess;
    private final ListeningExecutorService executorServiceDecorator;
    private final List<ListenableFuture<Void>> listenableFutures = new ArrayList<>();
    private final TransferCompleteHandler transferCompleteHandler;

    private volatile boolean shouldStop = false;

    public AbstractThrottlingStrategy(final WorkQueue<QueueContentType> workQueue,
                                      final ExecutorService executorService,
                                      final long numItemsToProcess,
                                      final TransferCompleteHandler transferCompleteHandler)
    {
        this.workQueue = workQueue;
        this.numItemsToProcess = numItemsToProcess;

        executorServiceDecorator = MoreExecutors.listeningDecorator(executorService);
        this.transferCompleteHandler = transferCompleteHandler;
    }

    protected void runInExecutor() {
        for (int i = 0; i < numItemsToProcess; ++i) {
            if (shouldStop) {
                break;
            }

            if ( ! executorServiceDecorator.isShutdown()) {
                listenableFutures.add(executorServiceDecorator.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        perform();
                        return null;
                    }
                }));
            }
        }

        try {
            Futures.allAsList(listenableFutures).get();
            signalTransferComplete();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            signalTransferComplete();
        } catch (final ExecutionException | CancellationException e) {
            signalTransferComplete();
        }
    }

    private final void signalTransferComplete() {
        executorServiceDecorator.shutdown();
        transferCompleteHandler.onTransferComplete();
    }

    protected void enqueueItem(final QueueContentType item) {
        try {
            workQueue.put(item);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected QueueContentType dequeueItem() {
        QueueContentType item = null;

        try {
            item = workQueue.take();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return item;
    }

    protected void perform() throws IOException {}

    public void stop() {
        shouldStop = true;
        final boolean interruptIfRunning = true;
        Futures.allAsList(listenableFutures).cancel(interruptIfRunning);
        signalTransferComplete();
    }

    protected boolean shouldStop() {
        return shouldStop;
    }
}

