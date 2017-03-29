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

package com.spectralogic.ds3client.helpers.strategy;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.JobNode;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public final class StrategyUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StrategyUtils.class);

    public static Ds3Client getClient(final ImmutableMap<UUID, JobNode> nodeMap, final UUID nodeId, final Ds3Client mainClient) {
        final JobNode jobNode = nodeMap.get(nodeId);

        if (jobNode == null) {
            LOG.warn("The jobNode was not found, returning the existing client");
            return mainClient;
        }

        return mainClient.newForNode(jobNode);
    }

    public static ImmutableMap<UUID, JobNode> buildNodeMap(final Iterable<JobNode> nodes) {
        final ImmutableMap.Builder<UUID, JobNode> nodeMap = ImmutableMap.builder();
        for (final JobNode node: nodes) {
            nodeMap.put(node.getId(), node);
        }
        return nodeMap.build();
    }

    /**
     * Filters out chunks that have already been completed.  We will get the same chunk name back from the server, but it
     * will not have any objects in it, so we remove that from the list of objects that are returned.
     * @param chunks The list to be filtered
     * @return The filtered list
     */
    public static ImmutableList<Objects> filterChunks(final Iterable<Objects> chunks) {
        final ImmutableList.Builder<Objects> builder = ImmutableList.builder();
        for (final Objects chunk : chunks) {
            final Objects filteredChunk = filterChunk(chunk);
            if (filteredChunk.getObjects().size() > 0) {
                builder.add(filteredChunk);
            }
        }
        return builder.build();
    }

    private static Objects filterChunk(final Objects chunk) {
        final Objects newChunk = new Objects();
        newChunk.setChunkId(chunk.getChunkId());
        newChunk.setChunkNumber(chunk.getChunkNumber());
        newChunk.setNodeId(chunk.getNodeId());
        newChunk.setObjects(filterObjects(chunk.getObjects()));
        return newChunk;
    }

    private static ImmutableList<BulkObject> filterObjects(final List<BulkObject> blobs) {
        final ImmutableList.Builder<BulkObject> builder = ImmutableList.builder();
        for (final BulkObject blob : blobs) {
            if (!blob.getInCache()) {
                builder.add(blob);
            }
        }
        return builder.build();
    }

    public static Path resolveForSymbolic(final Path path) throws IOException {
        if (Files.isSymbolicLink(path)) {
            final Path simLink = Files.readSymbolicLink(path);
            if (!simLink.isAbsolute()) {
                // Resolve the path such that the path is relative to the symbolically
                // linked file's directory
                final Path symLinkParent = path.toAbsolutePath().getParent();
                return symLinkParent.resolve(simLink);
            }

            return simLink;
        }
        return path;
    }

    public static ImmutableCollection<Range> getRangesForBlob(final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs,
                                                       final BulkObject blob) {
        final ImmutableMultimap<BulkObject, Range> rangesForBlob = rangesForBlobs.get(blob.getName());

        if (rangesForBlob != null) {
            return rangesForBlob.get(blob);
        }

        return null;
    }
}
