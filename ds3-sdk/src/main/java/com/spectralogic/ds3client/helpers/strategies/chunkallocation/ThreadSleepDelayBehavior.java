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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadSleepDelayBehavior implements DelayBehavior {
    private final static Logger LOGGER = LoggerFactory.getLogger(ThreadSleepDelayBehavior.class);

    private final int NUM_MILLI_SECONDS_IN_SECOND = 1000;

    private final Integer numSecondsToSleep;
    private final DelayBehaviorCallback delayBehaviorCallback;

    public ThreadSleepDelayBehavior(final Integer numSecondsToSleep, final DelayBehaviorCallback delayBehaviorCallback) {
        Preconditions.checkNotNull(delayBehaviorCallback, "delayBehaviorCallback may not be null.");

        if (numSecondsToSleep != null && numSecondsToSleep >= 0) {
            this.numSecondsToSleep = numSecondsToSleep;
        } else {
            this.numSecondsToSleep = null;
        }

        this.delayBehaviorCallback = delayBehaviorCallback;
    }

    @Override
    public void delay(final int numSecondsToDelay) {
        try {
            if (this.numSecondsToSleep != null) {
                delayBehaviorCallback.onBeforeDelay(this.numSecondsToSleep);
                Thread.sleep(this.numSecondsToSleep * NUM_MILLI_SECONDS_IN_SECOND);
            } else {
                delayBehaviorCallback.onBeforeDelay(numSecondsToDelay);
                Thread.sleep(numSecondsToDelay * NUM_MILLI_SECONDS_IN_SECOND);
            }
        } catch (final InterruptedException e) {
            LOGGER.info("Thread.sleep() interrupted: ", e.getMessage());
        }

    }
}
