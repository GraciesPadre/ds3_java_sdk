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
import com.spectralogic.ds3client.helpers.DataTransferredListener;

public class DataTransferredObserver implements Observer<Long> {
    private final DataTransferredListener dataTransferredListener;

    public DataTransferredObserver(final DataTransferredListener dataTransferredListener) {
        Preconditions.checkNotNull(dataTransferredListener, "dataTransferredListener may not be null.");

        this.dataTransferredListener = dataTransferredListener;
    }

    public DataTransferredListener getDataTransferredListener() {
        return dataTransferredListener;
    }

    @Override
    public void update(final Long eventData) {
        dataTransferredListener.dataTransferred(eventData);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof DataTransferredObserver)) return false;

        final DataTransferredObserver that = (DataTransferredObserver) o;

        return dataTransferredListener.equals(that.dataTransferredListener);
    }

    @Override
    public int hashCode() {
        return dataTransferredListener.hashCode();
    }
}
