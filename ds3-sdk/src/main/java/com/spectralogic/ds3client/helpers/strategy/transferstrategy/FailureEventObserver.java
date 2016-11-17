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
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;

public class FailureEventObserver implements Observer<FailureEvent> {
    private final FailureEventListener failureEventListener;

    public FailureEventObserver(final FailureEventListener failureEventListener) {
        Preconditions.checkNotNull(failureEventListener, "failureEventListener may not be null.");

        this.failureEventListener = failureEventListener;
    }

    @Override
    public void update(final FailureEvent eventData) {
        failureEventListener.onFailure(eventData);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof FailureEventObserver)) return false;

        final FailureEventObserver that = (FailureEventObserver) o;

        return failureEventListener.equals(that.failureEventListener);

    }

    @Override
    public int hashCode() {
        return failureEventListener.hashCode();
    }
}
