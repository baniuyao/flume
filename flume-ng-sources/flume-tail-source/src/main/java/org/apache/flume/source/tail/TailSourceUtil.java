package org.apache.flume.source.tail;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by ybaniu on 12/1/14.
 */
public class TailSourceUtil {
    private static final Logger log = LoggerFactory.getLogger(TailSourceUtil.class);
    public static Properties getTailProperties(Context context) {
        log.info("context={}", context);
        Properties tailProperties = new Properties();
        for (Map.Entry<String, String> prop : context.getSubProperties(TailSourceConstants.PROPERTY_PREFIX).entrySet()) {
            tailProperties.put(prop.getKey(), prop.getValue());
            log.info("get prop, key: {}, value: {}", prop.getKey(), prop.getValue());
        }
        return tailProperties;
    }
}
