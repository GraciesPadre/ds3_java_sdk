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

import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.EventRunner;

import java.util.Set;

public class EventBehaviorImpl implements EventBehavior {
    private final Set<WaitingForChunksListener> waitingForChunksListeners;
    private final EventRunner eventRunner;

    public EventBehaviorImpl(final Set<WaitingForChunksListener> waitingForChunksListeners,
                             final EventRunner eventRunner) {
        this.waitingForChunksListeners = waitingForChunksListeners;
        this.eventRunner = eventRunner;
    }

    @Override
    public void emitWaitingForChunksEvents(final int numSecondsToDelay) {
        for (final WaitingForChunksListener waitingForChunksListener : waitingForChunksListeners) {
            eventRunner.emitEvent(new Runnable() {
                @Override
                public void run() {
                    waitingForChunksListener.waiting(numSecondsToDelay);
                }
            });
        }
    }
}
