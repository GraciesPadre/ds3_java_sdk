/*
 * ******************************************************************************
 *   Copyright 2014-2016 Spectra Logic Corporation. All Rights Reserved.
 *   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *   this file except in compliance with the License. A copy of the License is located at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file.
 *   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *   CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *   specific language governing permissions and limitations under the License.
 * ****************************************************************************
 */

package com.spectralogic.ds3client.metadata;

import com.google.common.collect.ImmutableMap;
import com.spectralogic.ds3client.helpers.MetadataAccess;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.events.FailureActivity;
import com.spectralogic.ds3client.helpers.events.FailureEvent;
import com.spectralogic.ds3client.metadata.interfaces.FileMetadata;
import com.spectralogic.ds3client.metadata.interfaces.FileMetadataFactory;
import com.spectralogic.ds3client.utils.StringExtensions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class FileMetadataAccessImpl implements MetadataAccess {
    private final Path containingFolder;
    private final FailureEventListener failureEventListener;
    private final String httpEndpoint;
    private final FileMetadata fileMetadata;

    public FileMetadataAccessImpl(final Path containingFolder, final FailureEventListener failureEventListener, final String httpEndpoint)
    {
        this.containingFolder = containingFolder;
        this.failureEventListener = failureEventListener;
        this.httpEndpoint = httpEndpoint;

        fileMetadata = GuiceInjector.INSTANCE.injector().getInstance(FileMetadataFactory.class).fileMetadata();
    }

    public Map<String, String> getMetadataValue(final String filename) {
        try {
            return fileMetadata.readMetadataFrom(Paths.get(containingFolder.toString(), filename));
        } catch (final Throwable t) {
            if (failureEventListener != null) {
                failureEventListener.onFailure(FailureEvent.builder()
                        .withObjectNamed(filename)
                        .withCausalException(t)
                        .doingWhat(FailureActivity.RecordingMetadata)
                        .usingSystemWithEndpoint(StringExtensions.getStringOrDefault(httpEndpoint, ""))
                        .build());
            }
        }

        return ImmutableMap.of();
    }
}
