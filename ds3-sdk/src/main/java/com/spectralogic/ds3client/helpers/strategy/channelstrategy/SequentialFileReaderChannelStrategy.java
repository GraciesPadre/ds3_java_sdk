package com.spectralogic.ds3client.helpers.strategy.channelstrategy;

import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.BulkObjectList;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.ByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SequentialFileReaderChannelStrategy implements ChannelStrategy {
    private final Path directory;
    private BlobCompletionHandler blobCompletionHandler = null;

    public SequentialFileReaderChannelStrategy(final Path directory) {
        this.directory = directory;
    }

    @Override
    public BlobChannelPairs channelsForBlobs(final BulkObjectList blobs,
                                             final ChannelAllocationFailureHandler channelAllocationFailureHandler)
    {
        final BlobChannelPairs blobChannelPairs = new BlobChannelPairs();
        blobChannelPairs.prepare();

        try {
            populateBlobChannelPairs(blobs, channelAllocationFailureHandler, blobChannelPairs);
        } finally {
            blobChannelPairs.commit();
        }

        return blobChannelPairs;
    }

    private void populateBlobChannelPairs(final BulkObjectList blobs,
                                          final ChannelAllocationFailureHandler channelAllocationFailureHandler,
                                          final BlobChannelPairs blobChannelPairs)
    {
        for (final BulkObject blob : blobs.getObjects()) {
            try {
                final ByteChannel byteChannel = new FileInputStream(Paths.get(directory.toString(), blob.getName()).toFile()).getChannel();
                blobChannelPairs.add(blob, byteChannel);
            } catch (final FileNotFoundException e) {
                if (channelAllocationFailureHandler != null) {
                    channelAllocationFailureHandler.onChannelAllocationFailure(blob.getName(), e);
                }
            }
        }
    }

    @Override
    public void withBlobCompletionHandler(final BlobCompletionHandler blobCompletionHandler) {
        this.blobCompletionHandler = blobCompletionHandler;
    }

    @Override
    public void completeBlob(final BulkObject blob) {
        if (blobCompletionHandler != null) {
            blobCompletionHandler.onBlobComplete(blob);
        }
    }
}
