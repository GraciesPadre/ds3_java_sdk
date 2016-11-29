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
import org.slf4j.Logger;

public class DataTransferredObserver extends AbstractObserver<Long> {
    private DataTransferredListener dataTransferredListener;

    public DataTransferredObserver(final DataTransferredListener dataTransferredListener) {
        super(new UpdateStrategy<Long>() {
            @Override
            public void update(final Long eventData) {
                dataTransferredListener.dataTransferred(eventData);
            }
        });

        Preconditions.checkNotNull(dataTransferredListener, "dataTransferredListener may not be null.");

        this.dataTransferredListener = dataTransferredListener;
    }

    public DataTransferredObserver(final UpdateStrategy<Long> updateStrategy) {
        super(updateStrategy);
    }

    public DataTransferredListener getDataTransferredListener() {
        return dataTransferredListener;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof DataTransferredObserver)) return false;
        if (!super.equals(o)) return false;

        final DataTransferredObserver that = (DataTransferredObserver) o;

        return getDataTransferredListener() != null ? getDataTransferredListener().equals(that.getDataTransferredListener()) : that.getDataTransferredListener() == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getDataTransferredListener() != null ? getDataTransferredListener().hashCode() : 0);
        return result;
    }
}