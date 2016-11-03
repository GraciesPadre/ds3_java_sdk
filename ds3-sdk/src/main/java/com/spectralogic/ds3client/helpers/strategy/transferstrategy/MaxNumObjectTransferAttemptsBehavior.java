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

import com.google.common.base.Preconditions;
import com.spectralogic.ds3client.helpers.ExceptionClassifier;
import com.spectralogic.ds3client.helpers.JobPart;

import java.io.IOException;

public class MaxNumObjectTransferAttemptsBehavior implements TransferRetryBehavior {
    private final int maxNumObjectTransferAttempts;
    private DataTransceiver dataTransceiverDelegate;

    public MaxNumObjectTransferAttemptsBehavior(final int maxNumObjectTransferAttempts) {
        this.maxNumObjectTransferAttempts = maxNumObjectTransferAttempts;
    }

    @Override
    public DataTransceiver wrap(final DataTransceiver dataTransceiver) {
        Preconditions.checkNotNull(dataTransceiver, "dataTransceiver may not be null.");
        dataTransceiverDelegate = dataTransceiver;
        return this;
    }

    @Override
    public void transferJobPart(final JobPart jobPart) throws IOException {
        int objectTransfersAttempted = 0;

        while(true) {
            try {
                dataTransceiverDelegate.transferJobPart(jobPart);
                break;
            } catch (final Throwable t) {
                if (ExceptionClassifier.isUnrecoverableException(t) || ++objectTransfersAttempted >= maxNumObjectTransferAttempts) {
                    throw t;
                }
            }
        }
    }
}
