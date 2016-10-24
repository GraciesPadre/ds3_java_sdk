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

import com.spectralogic.ds3client.commands.interfaces.Ds3Request;
import com.spectralogic.ds3client.models.bulk.Ds3Object;

import javax.print.attribute.standard.JobState;

final class ObjectRequestFactory  implements ObjectRequestStrategy {
    private final JobState jobState;

    protected ObjectRequestFactory(final JobState jobState) {
        this.jobState = jobState;
    }

    @Override
    public Ds3Request makePutObjectRequest(final Ds3Object ds3Object) {
        return null;
    }

    @Override
    public Ds3Request makeGetObjectRequest(final Ds3Object ds3Object) {
        return null;
    }
}
