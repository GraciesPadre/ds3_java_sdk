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

package com.spectralogic.ds3client.helpers.strategies.chunkallocation;

import com.google.common.base.Preconditions;

import java.io.IOException;

public class MaxIterationRetryBehavior implements RetryBehavior {
    private final int maxNumIterations;
    private final RetryFailureCallback retryFailureCallback;

    private int currentIteration;

    public MaxIterationRetryBehavior(final int maxNumIterations, final RetryFailureCallback retryFailureCallback) {
        Preconditions.checkArgument(maxNumIterations >= 1, "maxNumIterations must be >= 1");
        Preconditions.checkNotNull(retryFailureCallback, "retryFailureCallback must not be null");

        this.maxNumIterations = maxNumIterations;
        this.retryFailureCallback = retryFailureCallback;
    }

    @Override
    public void processIteration() throws IOException {
        if (++currentIteration >= maxNumIterations) {
            retryFailureCallback.onFailure(currentIteration);
        }
    }

    @Override
    public void reset() {
        currentIteration = 0;
    }
}
