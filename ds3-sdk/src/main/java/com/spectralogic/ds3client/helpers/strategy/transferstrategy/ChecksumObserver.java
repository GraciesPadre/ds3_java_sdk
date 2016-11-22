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
import com.spectralogic.ds3client.helpers.ChecksumListener;

public class ChecksumObserver implements Observer<ChecksumEvent> {
    private  ChecksumListener checksumListener;
    private final UpdateStrategy<ChecksumEvent> updateStrategy;

    public ChecksumObserver(final ChecksumListener checksumListener) {
        Preconditions.checkNotNull(checksumListener, "checksumListener may not be null.");

        this.checksumListener = checksumListener;

        updateStrategy = new UpdateStrategy<ChecksumEvent>() {
            @Override
            public void update(final ChecksumEvent eventData) {
                checksumListener.value(eventData.getBlob(), eventData.getChecksumType(), eventData.getChecksum());
            }
        };
    }

    public ChecksumObserver(final UpdateStrategy<ChecksumEvent> updateStrategy) {
        Preconditions.checkNotNull(updateStrategy, "updateStrategy may not be null.");
        this.updateStrategy = updateStrategy;
    }

    @Override
    public void update(final ChecksumEvent eventData) {
        updateStrategy.update(eventData);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChecksumObserver)) return false;

        final ChecksumObserver that = (ChecksumObserver) o;

        if (checksumListener != null ? !checksumListener.equals(that.checksumListener) : that.checksumListener != null)
            return false;
        return updateStrategy.equals(that.updateStrategy);

    }

    @Override
    public int hashCode() {
        int result = checksumListener != null ? checksumListener.hashCode() : 0;
        result = 31 * result + updateStrategy.hashCode();
        return result;
    }
}
