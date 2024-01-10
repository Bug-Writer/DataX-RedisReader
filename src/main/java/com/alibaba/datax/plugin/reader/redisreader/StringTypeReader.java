package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class StringTypeReader extends RedisReadAbstract {

    public StringTypeReader(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void readData(RecordSender recordSender) {
        Jedis jedis = (Jedis) getRedisClient(configuration);
        String regexKey = configuration.getString(Key.INCLUDE);
        Pattern regexPattern;

        try {
            regexPattern = Pattern.compile(regexKey);
        }
        catch (PatternSyntaxException e) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "include项正则表达式非法：" + e.getMessage(), e);
        }

        try {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().count(1000); // 设置每次迭代返回的键的数量

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();

                for (String key : scanResult.getResult()) {
                    if (regexPattern.matcher(key).matches()) {
                        String value = jedis.get(key);
                        Record record = recordSender.createRecord();
                        Column column = new StringColumn(value);
                        record.addColumn(column);
                        recordSender.sendToWriter(record);
                    }
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        }
        finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
