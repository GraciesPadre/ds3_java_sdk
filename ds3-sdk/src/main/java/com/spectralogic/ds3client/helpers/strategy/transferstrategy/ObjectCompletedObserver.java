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
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;

public class ObjectCompletedObserver extends AbstractObserver<String> {
    private ObjectCompletedListener objectCompletedListener;

    public ObjectCompletedObserver(final ObjectCompletedListener objectCompletedListener) {
        super(new UpdateStrategy<String>() {
            @Override
            public void update(final String eventData) {
                objectCompletedListener.objectCompleted(eventData);
            }
        });

        Preconditions.checkNotNull(objectCompletedListener, "objectCompletedListener may not be null");

        this.objectCompletedListener = objectCompletedListener;
    }

    public ObjectCompletedObserver(final UpdateStrategy<String> updateStrategy) {
        super(updateStrategy);
    }
}