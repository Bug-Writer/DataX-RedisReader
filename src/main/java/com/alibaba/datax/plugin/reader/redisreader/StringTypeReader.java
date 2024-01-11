package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
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
        String regexExc = configuration.getString(Key.EXCLUDE);
        Pattern regexPattern, excluPattern;

        try {
            regexPattern = Pattern.compile(regexKey);
            excluPattern = Pattern.compile(regexExc);
        }
        catch (PatternSyntaxException e) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "正则表达式非法：" + e.getMessage(), e);
        }

        try {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().count(1000);
            List<Response<String>> responseList = new ArrayList<>();

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();

                Pipeline pipeline = jedis.pipelined();

                for (String key : scanResult.getResult()) {
                    if (regexPattern.matcher(key).matches() && !excluPattern.matcher(key).matches()) {
                        responseList.add(pipeline.get(key));
                    }
                }

                pipeline.sync();

                for (Response<String> response : responseList) {
                    String value = response.get();
                    Record record = recordSender.createRecord();
                    Column column = new StringColumn(value);
                    record.addColumn(column);
                    recordSender.sendToWriter(record);
                }
                responseList.clear();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        }
        finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
