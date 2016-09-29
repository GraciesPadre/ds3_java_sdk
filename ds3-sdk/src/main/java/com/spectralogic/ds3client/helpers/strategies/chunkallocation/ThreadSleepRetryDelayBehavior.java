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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadSleepRetryDelayBehavior implements RetryDelayBehavior {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final int numSecondsToSleep;

    public ThreadSleepRetryDelayBehavior(final int numSecondsToSleep) {
        if (numSecondsToSleep < 1) {
            throw new IllegalArgumentException("numSecondsToSleep must be >= 1");
        }

        this.numSecondsToSleep = numSecondsToSleep;
    }

    @Override
    public void delay() {
        try {
            Thread.sleep(numSecondsToSleep * 1000);
        } catch (final InterruptedException e) {
            LOG.info("Thread.sleep() interrupted");
        }
    }
}
