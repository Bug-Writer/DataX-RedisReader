package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class StringTypeReader extends RedisReadAbstract {

    public StringTypeReader(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void readData(RecordSender recordSender) {
        Jedis jedis = (Jedis) getRedisClient(configuration);
        Pipeline pipeline = jedis.pipelined();
        String redisKey = Key.INCLUDE;

        String redisValue = pipeline.get(redisKey).get();

        Record record = recordSender.createRecord();
        Column column = new StringColumn(redisValue);
        record.addColumn(column);

        recordSender.sendToWriter(record);
    }
}
