package com.ververica.flink.table.gateway.config.entries;

import com.ververica.flink.table.gateway.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.ververica.flink.table.gateway.config.Environment.YANA_ENTRY;


public class YanaEntry extends ConfigEntry {

    private static final Logger LOG = LoggerFactory.getLogger(YanaEntry.class);

    public static final YanaEntry DEFAULT_INSTANCE =
            new YanaEntry(new DescriptorProperties(true));

    private static final String defaultDatabase = "defaultDatabase";
    private static final String username = "username";
    private static final String pwd = "pwd";
    private static final String connectUrl = "connectUrl";

    private YanaEntry(DescriptorProperties properties) {
        super(properties);
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        //todo
//        properties.validateString(defaultDatabase, false, 1);
//        properties.validateString(username, false, 1);
//        properties.validateString(pwd, false, 1);
//        properties.validateString(connectUrl, false, 1);
    }

    public String getDefaultDatabase() {
        return properties.getOptionalString(defaultDatabase).orElseGet(() -> useDefaultValue(defaultDatabase, "data-plat"));
    }

    public String getUsername() {
        return properties.getOptionalString(username).orElseGet(() -> useDefaultValue(username, "root"));
    }

    public String getPwd() {
        return properties.getOptionalString(pwd).orElseGet(() -> useDefaultValue(pwd, "Password01!"));
    }

    public String getConnectUrl() {
        return properties.getOptionalString(connectUrl).orElseGet(() -> useDefaultValue(connectUrl, "jdbc:mysql://120.27.2.6:3306/data-plat?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"));
    }


    private <V> V useDefaultValue(String key, V defaultValue) {
        LOG.info("Property '{}.{}' not specified. Using default value: {}", YANA_ENTRY, key, defaultValue);
        return defaultValue;
    }

    public Map<String, String> asTopLevelMap() {
        return properties.asPrefixedMap(YANA_ENTRY + '.');
    }


    public static YanaEntry create(Map<String, Object> config) {
        return new YanaEntry(ConfigUtil.normalizeYaml(config));
    }

    @Override
    public String toString() {
        return "YanaEntry{" + "properties=" + properties + '}';
    }
}
