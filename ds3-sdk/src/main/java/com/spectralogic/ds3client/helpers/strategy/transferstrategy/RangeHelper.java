package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.utils.Guard;

final class RangeHelper {
    private RangeHelper() {}

    static ImmutableCollection<Range> replaceRange(final ImmutableCollection<Range> existingRanges,
                                                   final long numBytesTransferred,
                                                   final long intendedNumBytesToTransfer)
    {
        Preconditions.checkState(numBytesTransferred >= 0, "numBytesTransferred must be >= 0.");
        Preconditions.checkState(intendedNumBytesToTransfer > 0, "intendedNumBytesToTransfer must be > 0.");
        Preconditions.checkState(intendedNumBytesToTransfer > numBytesTransferred, "intendedNumBytesToTransfer must be > numBytesTransferred");

        if (Guard.isNullOrEmpty(existingRanges)) {
            return ImmutableList.of(Range.byLength(numBytesTransferred, intendedNumBytesToTransfer - numBytesTransferred));
        }

        final ImmutableList.Builder<Range> newRangesbuilder = ImmutableList.builder();

        final UnmodifiableIterator<Range> existingRangesIterator = existingRanges.iterator();

        long previousAccumulatedBytesInRanges = 0;
        long currentAccumulatedBytesInRanges = existingRanges.iterator().next().getLength();

        while (existingRangesIterator.hasNext()) {
            final Range existingRange = existingRangesIterator.next();

            if (numBytesTransferred < currentAccumulatedBytesInRanges) {
                final Range firstNewRange = Range.byPosition(existingRange.getStart() - previousAccumulatedBytesInRanges  + numBytesTransferred, existingRange.getEnd());
                newRangesbuilder.add(firstNewRange);

                addRemainingRanges(existingRangesIterator, newRangesbuilder);
                break;
            }

            previousAccumulatedBytesInRanges += existingRange.getLength();
            currentAccumulatedBytesInRanges += existingRange.getLength();
        }

        return newRangesbuilder.build();
    }

    private static void addRemainingRanges(final UnmodifiableIterator<Range> existingRangesIterator, final ImmutableList.Builder<Range> newRangesbuilder) {
        while (existingRangesIterator.hasNext()) {
            newRangesbuilder.add(existingRangesIterator.next());
        }
    }

    static long transferSizeForRanges(final ImmutableCollection<Range> existingRanges) {
        long result = 0;

        if (Guard.isNullOrEmpty(existingRanges)) {
            return result;
        }

        for (final Range range : existingRanges) {
            result += range.getLength();
        }

        return result;
    }
}