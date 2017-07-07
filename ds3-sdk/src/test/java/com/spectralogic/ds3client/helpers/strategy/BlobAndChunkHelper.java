/*
 * ******************************************************************************
 *   Copyright 2014-2017 Spectra Logic Corporation. All Rights Reserved.
 *   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *   this file except in compliance with the License. A copy of the License is located at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file.
 *   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *   CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *   specific language governing permissions and limitations under the License.
 * ****************************************************************************
 */

package com.spectralogic.ds3client.helpers.strategy;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.MasterObjectList;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.bulk.Ds3Object;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

final class BlobAndChunkHelper {
    static final String Trixie = "Trixie";
    static final String Shasta = "Shasta";
    static final String Gracie = "Gracie";
    static final String Twitch = "Twitch";
    static final String Marbles = "Marbles";
    static final String Nibbles = "Nibbles";

    static final String bucketName = "bucket";

    static List<Ds3Object> makeObjectsInFirstChunk() {
        final Ds3Object trixie = new Ds3Object(Trixie, 1);
        final Ds3Object shasta = new Ds3Object(Shasta, 2);
        final Ds3Object gracie = new Ds3Object(Gracie, 3);

        return Arrays.asList(trixie, shasta, gracie);
    }

    static BulkObject makeBlob(final long size, final String name) {
        final BulkObject blob = new BulkObject();
        blob.setBucket(bucketName);
        blob.setId(UUID.randomUUID());
        blob.setInCache(false);
        blob.setLatest(true);
        blob.setLength(size);
        blob.setName(name);
        blob.setOffset(0);
        blob.setVersion(1);

        return blob;
    }

    static Objects makeChunk(final int chunkNumber, final List<BulkObject> blobs) {
        final Objects chunk = new Objects();
        chunk.setChunkId(UUID.randomUUID());
        chunk.setChunkNumber(chunkNumber);
        chunk.setNodeId(UUID.randomUUID());
        chunk.setObjects(blobs);

        return chunk;
    }

    static List<Ds3Object> makeObjectsInSecondChunk() {
        final Ds3Object twitch = new Ds3Object(Twitch, 4);
        final Ds3Object marbles = new Ds3Object(Marbles, 5);
        final Ds3Object nibbles = new Ds3Object(Nibbles, 6);

        return Arrays.asList(twitch, marbles, nibbles);
    }

    static long sizeOfObjectsInFirstChunk() {
        final AtomicLong objectSize = new AtomicLong(0);

        FluentIterable.from(makeObjectsInFirstChunk())
                .transform(new Function<Ds3Object, Long>() {
                    @Nullable
                    @Override
                    public Long apply(@Nullable final Ds3Object ds3Object) {
                        objectSize.addAndGet(ds3Object.getSize());
                        return ds3Object.getSize();
                    }
                });

        return objectSize.get();
    }

    static boolean masterObjectListsAreEqual(final MasterObjectList left, final MasterObjectList right) {
        return left.getAggregating() == right.getAggregating() &&
                left.getBucketName().equals(right.getBucketName()) &&
                left.getCachedSizeInBytes() == right.getCachedSizeInBytes() &&
                left.getChunkClientProcessingOrderGuarantee().equals(right.getChunkClientProcessingOrderGuarantee()) &&
                left.getCompletedSizeInBytes() == right.getCompletedSizeInBytes() &&
                left.getEntirelyInCache() == right.getEntirelyInCache() &&
                left.getJobId().equals(right.getJobId()) &&
                left.getNaked() == right.getNaked() &&
                left.getName().equals(right.getName()) &&
                left.getNodes().equals(right.getNodes()) &&
                left.getObjects().equals(right.getObjects()) &&
                left.getOriginalSizeInBytes() == right.getOriginalSizeInBytes() &&
                left.getPriority().equals(right.getPriority()) &&
                left.getRequestType().equals(right.getRequestType()) &&
                left.getStartDate().equals(right.getStartDate()) &&
                left.getStatus().equals(right.getStatus()) &&
                left.getUserId().equals(right.getUserId()) &&
                left.getUserName().equals(right.getUserName());
    }
}
