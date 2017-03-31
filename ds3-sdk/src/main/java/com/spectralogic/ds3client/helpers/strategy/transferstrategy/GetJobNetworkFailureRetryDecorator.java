package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.spectralogic.ds3client.exceptions.ContentLengthNotMatchException;
import com.spectralogic.ds3client.helpers.JobPart;
import com.spectralogic.ds3client.helpers.RecoverableIOException;
import com.spectralogic.ds3client.helpers.strategy.StrategyUtils;
import com.spectralogic.ds3client.helpers.strategy.channelstrategy.ChannelStrategy;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.common.Range;

import java.io.IOException;

/**
 * An implementation of {@link TransferMethod} that wraps another instance of TransferMethod to provide the
 * ability to resume a get operation that transfers less data than is needed for a blob transfer to complete.
 */
public class GetJobNetworkFailureRetryDecorator implements TransferMethod {
    private final ChannelStrategy channelStrategy;
    private final String bucketName;
    private final String jobId;
    private final EventDispatcher eventDispatcher;

    private ImmutableCollection<Range> ranges;
    private Long numBytesToTransfer;
    private Long destinationChannelOffset = 0L;
    private final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs;

    private TransferMethod transferMethod;

    public GetJobNetworkFailureRetryDecorator(final ChannelStrategy channelStrategy,
                                              final String bucketName,
                                              final String jobId,
                                              final EventDispatcher eventDispatcher,
                                              final ImmutableMap<String, ImmutableMultimap<BulkObject, Range>> rangesForBlobs)
    {
        this.channelStrategy = channelStrategy;
        this.bucketName = bucketName;
        this.jobId = jobId;
        this.eventDispatcher = eventDispatcher;
        this.rangesForBlobs = rangesForBlobs;

        transferMethod = new GetJobTransferMethod(channelStrategy, bucketName, jobId, eventDispatcher, rangesForBlobs);
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        try {
            transferMethod.transferJobPart(jobPart);
        } catch (final ContentLengthNotMatchException contentLengthNotMatchException) {
            makeNewTransferMethod(jobPart.getBulkObject(), contentLengthNotMatchException.getTotalBytes());
            eventDispatcher.emitContentLengthMismatchFailureEvent(jobPart.getBulkObject(),
                    jobPart.getClient().getConnectionDetails().getEndpoint(),
                    contentLengthNotMatchException);
            throw new RecoverableIOException(contentLengthNotMatchException);
        }
    }

    private void makeNewTransferMethod(final BulkObject ds3Object, final long numBytesTransferred) {
        initializeRangesAndTransferSize(ds3Object);
        updateRanges(numBytesTransferred);
        destinationChannelOffset += numBytesTransferred;

        transferMethod = new GetJobPartialBlobTransferMethod(channelStrategy, bucketName, jobId,
                eventDispatcher, ranges, destinationChannelOffset);
    }

    private void initializeRangesAndTransferSize(final BulkObject blob) {
        if (ranges == null) {
            ranges = StrategyUtils.getRangesForBlob(rangesForBlobs, blob);
        }

        if (ranges == null) {
            final long numBytesTransferred = 0;
            ranges = RangeHelper.replaceRange(ranges, numBytesTransferred, blob.getLength());
        }

        if (numBytesToTransfer == null) {
            numBytesToTransfer = RangeHelper.transferSizeForRanges(ranges);
        }
    }

    private void updateRanges(final long numBytesTransferred) {
        ranges = RangeHelper.replaceRange(ranges, numBytesTransferred, numBytesToTransfer);
    }
}
