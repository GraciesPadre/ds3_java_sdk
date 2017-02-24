package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.GetObjectResponse;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.common.Range;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class GetJobPartialBlobTransferMethod implements TransferMethod {
    private final ChannelStrategy channelStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventDispatcher eventDispatcher;
    private final ImmutableCollection<Range> blobRanges;
    private final long destinationChannelOffset;

    public GetJobPartialBlobTransferMethod(final ChannelStrategy channelStrategy,
                                           final String bucketName,
                                           final String jobId,
                                           final EventDispatcher eventDispatcher,
                                           final ImmutableCollection<Range> blobRanges,
                                           final long destinationChannelOffset)
    {
        Preconditions.checkNotNull(blobRanges, "blobRanges may not be null.");
        Preconditions.checkState(destinationChannelOffset >= 0, "destinationChannelOffset must be >= 0.");

        this.channelStrategy = channelStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventDispatcher = eventDispatcher;
        this.blobRanges = blobRanges;
        this.destinationChannelOffset = destinationChannelOffset;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        final BulkObject blob = jobPart.getBulkObject();

        final SeekableByteChannel seekableByteChannel = channelStrategy.acquireChannelForBlob(blob);

        seekableByteChannel.position(destinationChannelOffset);

        try {
            final GetObjectResponse getObjectResponse = jobPart.getClient().getObject(makeGetObjectRequest(seekableByteChannel, jobPart));

            eventDispatcher.emitChecksumEvent(blob, getObjectResponse.getChecksumType(), getObjectResponse.getChecksum());
            eventDispatcher.emitMetaDataReceivedEvent(blob.getName(), getObjectResponse.getMetadata());

            eventDispatcher.emitBlobTransferredEvent(blob);
            eventDispatcher.emitDataTransferredEvent(blob);
        } finally {
            channelStrategy.releaseChannelForBlob(seekableByteChannel, blob);
        }
    }

    private GetObjectRequest makeGetObjectRequest(final SeekableByteChannel seekableByteChannel, final JobPart jobPart) {
        final BulkObject blob = jobPart.getBulkObject();

        final GetObjectRequest getObjectRequest = new GetObjectRequest(
                bucketName,
                blob.getName(),
                seekableByteChannel,
                jobId,
                blob.getOffset());

        getObjectRequest.withByteRanges(blobRanges);

        return getObjectRequest;
    }
}
