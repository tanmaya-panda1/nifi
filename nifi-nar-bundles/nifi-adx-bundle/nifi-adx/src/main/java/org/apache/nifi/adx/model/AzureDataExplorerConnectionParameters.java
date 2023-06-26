/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.adx.model;

public class AzureDataExplorerConnectionParameters {

    private final String kustoAuthStrategy;
    private final String appId;
    private final String appKey;
    private final String appTenant;
    private final String kustoEngineURL;
    private final String kustoIngestionURL;

    public AzureDataExplorerConnectionParameters(String kustoAuthStrategy, String appId, String appKey, String appTenant, String kustoEngineURL, String kustoIngestionURL) {
        this.kustoAuthStrategy = kustoAuthStrategy;
        this.appId = appId;
        this.appKey = appKey;
        this.appTenant = appTenant;
        this.kustoEngineURL = kustoEngineURL;
        this.kustoIngestionURL = kustoIngestionURL;
    }

    public AzureDataExplorerConnectionParameters(String kustoAuthStrategy, String appId, String appKey, String appTenant, String kustoEngineURL) {
        this.kustoAuthStrategy = kustoAuthStrategy;
        this.appId = appId;
        this.appKey = appKey;
        this.appTenant = appTenant;
        this.kustoEngineURL = kustoEngineURL;
        this.kustoIngestionURL = null;
    }

    public String getKustoAuthStrategy() {
        return kustoAuthStrategy;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getAppTenant() {
        return appTenant;
    }

    public String getKustoEngineURL() {
        return kustoEngineURL;
    }

    public String getKustoIngestionURL() {
        return kustoIngestionURL;
    }
}
