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

import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;

import java.io.IOException;

public interface TransferStrategy extends EventDispatcher {
    void transfer() throws IOException, InterruptedException;

    void attachChecksumObserver(final ChecksumObserver checksumObserver);
    void removeChecksumObserver(final ChecksumObserver checksumObserver);
    void emitChecksumEvent(final BulkObject blob, final ChecksumType.Type type, final String checksum);
}
