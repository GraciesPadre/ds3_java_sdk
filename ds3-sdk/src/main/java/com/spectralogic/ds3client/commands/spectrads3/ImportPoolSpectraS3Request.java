/*
 * ******************************************************************************
 *   Copyright 2014-2015 Spectra Logic Corporation. All Rights Reserved.
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

// This code is auto-generated, do not modify
package com.spectralogic.ds3client.commands.spectrads3;

import com.spectralogic.ds3client.networking.HttpVerb;
import com.spectralogic.ds3client.commands.interfaces.AbstractRequest;
import com.spectralogic.ds3client.models.ImportConflictResolutionMode;
import java.util.UUID;
import com.google.common.net.UrlEscapers;

public class ImportPoolSpectraS3Request extends AbstractRequest {

    // Variables
    
    private final String pool;

    private ImportConflictResolutionMode conflictResolutionMode;

    private String dataPolicyId;

    private String storageDomainId;

    private String userId;

    // Constructor
    
    public ImportPoolSpectraS3Request(final String pool) {
        this.pool = pool;
        
        this.getQueryParams().put("operation", "import");

    }

    public ImportPoolSpectraS3Request withConflictResolutionMode(final ImportConflictResolutionMode conflictResolutionMode) {
        this.conflictResolutionMode = conflictResolutionMode;
        this.updateQueryParam("conflict_resolution_mode", conflictResolutionMode);
        return this;
    }

    public ImportPoolSpectraS3Request withDataPolicyId(final UUID dataPolicyId) {
        this.dataPolicyId = dataPolicyId.toString();
        this.updateQueryParam("data_policy_id", dataPolicyId);
        return this;
    }

    public ImportPoolSpectraS3Request withDataPolicyId(final String dataPolicyId) {
        this.dataPolicyId = dataPolicyId;
        this.updateQueryParam("data_policy_id", dataPolicyId);
        return this;
    }

    public ImportPoolSpectraS3Request withStorageDomainId(final UUID storageDomainId) {
        this.storageDomainId = storageDomainId.toString();
        this.updateQueryParam("storage_domain_id", storageDomainId);
        return this;
    }

    public ImportPoolSpectraS3Request withStorageDomainId(final String storageDomainId) {
        this.storageDomainId = storageDomainId;
        this.updateQueryParam("storage_domain_id", storageDomainId);
        return this;
    }

    public ImportPoolSpectraS3Request withUserId(final UUID userId) {
        this.userId = userId.toString();
        this.updateQueryParam("user_id", userId);
        return this;
    }

    public ImportPoolSpectraS3Request withUserId(final String userId) {
        this.userId = userId;
        this.updateQueryParam("user_id", userId);
        return this;
    }


    @Override
    public HttpVerb getVerb() {
        return HttpVerb.PUT;
    }

    @Override
    public String getPath() {
        return "/_rest_/pool/" + pool;
    }
    
    public String getPool() {
        return this.pool;
    }


    public ImportConflictResolutionMode getConflictResolutionMode() {
        return this.conflictResolutionMode;
    }


    public String getDataPolicyId() {
        return this.dataPolicyId;
    }


    public String getStorageDomainId() {
        return this.storageDomainId;
    }


    public String getUserId() {
        return this.userId;
    }

}