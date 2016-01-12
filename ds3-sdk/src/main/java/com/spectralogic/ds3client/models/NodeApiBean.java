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
package com.spectralogic.ds3client.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.String;
import java.lang.Integer;
import java.util.UUID;

public class NodeApiBean {

    // Variables
    @JsonProperty("EndPoint")
    private String endPoint;

    @JsonProperty("HttpPort")
    private Integer httpPort;

    @JsonProperty("HttpsPort")
    private Integer httpsPort;

    @JsonProperty("Id")
    private UUID id;

    // Constructor
    public NodeApiBean(final String endPoint, final Integer httpPort, final Integer httpsPort, final UUID id) {
        this.endPoint = endPoint;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
        this.id = id;
    }

    // Getters and Setters
    
    public String getEndPoint() {
        return this.endPoint;
    }

    public void setEndPoint(final String endPoint) {
        this.endPoint = endPoint;
    }


    public Integer getHttpPort() {
        return this.httpPort;
    }

    public void setHttpPort(final Integer httpPort) {
        this.httpPort = httpPort;
    }


    public Integer getHttpsPort() {
        return this.httpsPort;
    }

    public void setHttpsPort(final Integer httpsPort) {
        this.httpsPort = httpsPort;
    }


    public UUID getId() {
        return this.id;
    }

    public void setId(final UUID id) {
        this.id = id;
    }

}