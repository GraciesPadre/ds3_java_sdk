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

package com.spectralogic.ds3client.helpers.strategy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import static com.spectralogic.ds3client.helpers.strategy.StrategyUtils.makeResettableInputStream;

public class UrlObjectInputStreamBuilder implements ObjectInputStreamBuilder {
    @Override
    public InputStream buildObjectStream(final String objectName) throws IOException {
        final URL url = new URL(objectName);
        final URLConnection urlConnection = url.openConnection();
        urlConnection.setDoInput(true);

        final int offset = 0;

        return makeResettableInputStream(urlConnection.getInputStream(), offset);
    }
}
