package com.spectralogic.ds3client.helpers.strategy.transferstrategy;

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.helpers.ExceptionClassifier;
import com.spectralogic.ds3client.helpers.JobPart;

import java.io.IOException;

public class ContinueForeverTransferRetryDecorator implements TransferRetryDecorator {
    private TransferMethod transferMethodDelegate;

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        while(true) {
            try {
                transferMethodDelegate.transferJobPart(jobPart);
                break;
            } catch (final Throwable t) {
                if (ExceptionClassifier.isUnrecoverableException(t)) {
                    throw t;
                }
            }
        }
    }

    @Override
    public TransferMethod wrap(final TransferMethod transferMethod) {
        Preconditions.checkNotNull(transferMethod, "transferMethod may not be null.");
        transferMethodDelegate = transferMethod;
        return this;
    }
}
