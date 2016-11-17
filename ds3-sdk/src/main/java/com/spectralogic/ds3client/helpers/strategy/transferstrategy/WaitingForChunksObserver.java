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
import com.spectralogic.ds3client.helpers.WaitingForChunksListener;

public class WaitingForChunksObserver implements Observer<Integer> {
    private final WaitingForChunksListener waitingForChunksListener;

    public WaitingForChunksObserver(final WaitingForChunksListener waitingForChunksListener) {
        Preconditions.checkNotNull(waitingForChunksListener, "waitingForChunksListener may not be null.");

        this.waitingForChunksListener = waitingForChunksListener;
    }

    @Override
    public void update(final Integer eventData) {
        waitingForChunksListener.waiting(eventData);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof WaitingForChunksObserver)) return false;

        final WaitingForChunksObserver that = (WaitingForChunksObserver) o;

        return waitingForChunksListener.equals(that.waitingForChunksListener);

    }

    @Override
    public int hashCode() {
        return waitingForChunksListener.hashCode();
    }
}
