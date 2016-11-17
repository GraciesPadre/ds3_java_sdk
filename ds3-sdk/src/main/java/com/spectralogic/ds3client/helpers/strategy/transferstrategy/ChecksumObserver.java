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
    private final ChecksumListener checksumListener;

    public ChecksumObserver(final ChecksumListener checksumListener) {
        Preconditions.checkNotNull(checksumListener, "checksumListener may not be null.");

        this.checksumListener = checksumListener;
    }

    @Override
    public void update(final ChecksumEvent eventData) {
        checksumListener.value(eventData.getBlob(), eventData.getChecksumType(), eventData.getChecksum());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ChecksumObserver)) return false;

        final ChecksumObserver that = (ChecksumObserver) o;

        return checksumListener.equals(that.checksumListener);
    }

    @Override
    public int hashCode() {
        return checksumListener.hashCode();
    }
}
