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

public class WaitingForChunksObserver extends AbstractObserver<Integer> {
    private WaitingForChunksListener waitingForChunksListener;

    public WaitingForChunksObserver(final WaitingForChunksListener waitingForChunksListener) {
        super(new UpdateStrategy<Integer>() {
            @Override
            public void update(final Integer eventData) {
                waitingForChunksListener.waiting(eventData);
            }
        });

        Preconditions.checkNotNull(waitingForChunksListener, "waitingForChunksListener may not be null.");

        this.waitingForChunksListener = waitingForChunksListener;
    }

    public WaitingForChunksObserver(final UpdateStrategy<Integer> updateStrategy) {
        super(updateStrategy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WaitingForChunksObserver)) return false;
        if (!super.equals(o)) return false;

        WaitingForChunksObserver that = (WaitingForChunksObserver) o;

        return waitingForChunksListener != null ? waitingForChunksListener.equals(that.waitingForChunksListener) : that.waitingForChunksListener == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (waitingForChunksListener != null ? waitingForChunksListener.hashCode() : 0);
        return result;
    }
}
