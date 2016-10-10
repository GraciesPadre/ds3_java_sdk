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

package com.spectralogic.ds3client.helpers;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.strategies.chunktransfer.ItemTransferrer;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.JobNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

class ChunkTransferrer {
    private final static Logger LOG = LoggerFactory.getLogger(ChunkTransferrer.class);
    private final ItemTransferrer itemTransferrer;
    private final Ds3Client mainClient;
    private final JobPartTracker partTracker;
    private final int maxParallelRequests;

    public ChunkTransferrer(
            final ItemTransferrer transferrer,
            final Ds3Client mainClient,
            final JobPartTracker partTracker,
            final int maxParallelRequests) {
        this.itemTransferrer = transferrer;
        this.mainClient = mainClient;
        this.partTracker = partTracker;
        this.maxParallelRequests = maxParallelRequests;
    }

    public void transferChunks(
            final Iterable<JobNode> nodes,
            final Iterable<Objects> chunks)
                throws IOException {
        LOG.debug("Getting ready to process chunks");
        final ImmutableMap<UUID, JobNode> nodeMap = buildNodeMap(nodes);
        LOG.debug("Starting executor service");
        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxParallelRequests));
        LOG.debug("Executor service started");
        try {
            final List<ListenableFuture<?>> tasks = new ArrayList<>();
            for (final Objects chunk : chunks) {
                LOG.debug("Processing parts for chunk: {}", chunk.getChunkId().toString());
                final Ds3Client client = getClient(nodeMap, chunk.getNodeId(), mainClient);
                for (final BulkObject ds3Object : chunk.getObjects()) {
                    final ObjectPart part = new ObjectPart(ds3Object.getOffset(), ds3Object.getLength());
                    if (this.partTracker.containsPart(ds3Object.getName(), part)) {
                        LOG.debug("Adding {} to executor for processing", ds3Object.getName());
                        tasks.add(executor.submit(new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                                LOG.debug("Processing {}", ds3Object.getName());
                                ChunkTransferrer.this.itemTransferrer.transferItem(client, ds3Object);
                                ChunkTransferrer.this.partTracker.completePart(ds3Object.getName(), part);
                                return null;
                            }
                        }));
                    }
                }
            }
            executeWithExceptionHandling(tasks);
        } finally {
            LOG.debug("Shutting down executor");
            executor.shutdown();
        }
    }

    private static Ds3Client getClient(final ImmutableMap<UUID, JobNode> nodeMap, final UUID nodeId, final Ds3Client mainClient) {
        final JobNode jobNode = nodeMap.get(nodeId);

        if (jobNode == null) {
            LOG.warn("The jobNode was not found, returning the existing client");
            return mainClient;
        }

        return mainClient.newForNode(jobNode);
    }

    private static ImmutableMap<UUID, JobNode> buildNodeMap(final Iterable<JobNode> nodes) {
        final ImmutableMap.Builder<UUID, JobNode> nodeMap = ImmutableMap.builder();
        for (final JobNode node: nodes) {
            nodeMap.put(node.getId(), node);
        }
        return nodeMap.build();
    }

    private static void executeWithExceptionHandling(final List<ListenableFuture<?>> tasks)
            throws IOException {
        try {
            // Block on the asynchronous result.
            Futures.allAsList(tasks).get();
        } catch (final InterruptedException e) {
            // This is a checked exception, but we don't want to expose that this is implemented with futures.
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
}
