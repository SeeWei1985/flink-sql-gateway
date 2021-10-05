/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.gateway.config.entries;

import com.ververica.flink.table.gateway.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Collections;
import java.util.Map;

import static com.ververica.flink.table.gateway.config.Environment.YANA_ENTRY;

/**
 * Describes a catalog configuration entry.
 */
public class YanaEntry extends ConfigEntry {

    public static final YanaEntry DEFAULT_INSTANCE = new YanaEntry(
            "data-plat",
            "root",
            "root",
            "jdbc:mysql://localhost:3306/data-plat?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC",
            new DescriptorProperties(true)
    );

    public static final String YANA_NAME = "yana";

    private final String defaultDatabase;
    private final String username;
    private final String pwd;
    private final String connectUrl;

    protected YanaEntry(String defaultDatabase, String username, String pwd, String connectUrl, DescriptorProperties properties) {
        super(properties);
        this.defaultDatabase = defaultDatabase;
        this.username = username;
        this.pwd = pwd;
        this.connectUrl = connectUrl;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getUsername() {
        return username;
    }

    public String getPwd() {
        return pwd;
    }

    public String getConnectUrl() {
        return connectUrl;
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        //properties.validateString(CATALOG_TYPE, false, 1);
        //properties.validateInt(CATALOG_PROPERTY_VERSION, true, 0);

        // further validation is performed by the discovered factory
    }

    public static YanaEntry create(Map<String, Object> config) {
        return create(ConfigUtil.normalizeYaml(config));
    }

    public Map<String, String> asTopLevelMap() {
        return properties.asPrefixedMap(YANA_ENTRY + '.');
    }

    private static YanaEntry create(DescriptorProperties properties) {


        // properties.validateString(CATALOG_NAME, false, 1);

        final String defaultDatabase = properties.getString("defaultDatabase");
        final String username = properties.getString("username");
        final String pwd = properties.getString("pwd");
        final String connectUrl = properties.getString("connectUrl");

        final DescriptorProperties cleanedProperties =
                properties.withoutKeys(Collections.singletonList(YANA_NAME));

        return new YanaEntry(defaultDatabase, username, pwd, connectUrl, cleanedProperties);
    }
}
