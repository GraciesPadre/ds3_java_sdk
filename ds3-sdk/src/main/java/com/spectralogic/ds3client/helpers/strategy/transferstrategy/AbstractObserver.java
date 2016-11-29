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

public abstract class AbstractObserver<T> implements Observer<T> {
    private final UpdateStrategy<T> updateStrategy;

    public AbstractObserver(final UpdateStrategy<T> updateStrategy) {
        Preconditions.checkNotNull(updateStrategy, "updateStrategy may not be null.");
        this.updateStrategy = updateStrategy;
    }

    @Override
    public void update(final T eventData) {
        updateStrategy.update(eventData);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractObserver)) return false;

        final AbstractObserver<?> that = (AbstractObserver<?>) o;

        return updateStrategy.equals(that.updateStrategy);

    }

    @Override
    public int hashCode() {
        return updateStrategy.hashCode();
    }
}