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

public class ChunkAllocationRetryBehaviorImpl implements ChunkAllocationRetryBehavior {
    private final RetryDelayBehavior retryDelayBehavior;
    private final RetryIterationBehavior retryIterationBehavior;

    public ChunkAllocationRetryBehaviorImpl(final RetryDelayBehavior retryDelayBehavior,
                                            final RetryIterationBehavior retryIterationBehavior) {
        Preconditions.checkNotNull(retryDelayBehavior);
        Preconditions.checkNotNull(retryIterationBehavior);

        this.retryDelayBehavior = retryDelayBehavior;
        this.retryIterationBehavior = retryIterationBehavior;
    }

    @Override
    public void delay() {
        retryDelayBehavior.delay();
    }

    @Override
    public void reset() {
        retryIterationBehavior.reset();
    }

    @Override
    public void processIteration() {
        retryIterationBehavior.processIteration();
    }
}
