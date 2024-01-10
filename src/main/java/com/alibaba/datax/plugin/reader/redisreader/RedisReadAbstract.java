package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public abstract class RedisReadAbstract {
    protected Configuration configuration;
    Object redisClient;
    protected int records;

    public RedisReadAbstract(Configuration configuration) {
        this.configuration = configuration;
    }

    Object getRedisClient(Configuration configuration) {
        String mode = configuration.getNecessaryValue(Key.MODE, CommonErrorCode.CONFIG_ERROR);
        String addr = configuration.getNecessaryValue(Key.ADDRESS, CommonErrorCode.CONFIG_ERROR);
        String auth = configuration.getString(Key.AUTH);
        if (Constant.CLUSTER.equalsIgnoreCase(mode)) {
            redisClient = RedisReaderHelper.getJedisCluster(addr, auth);
        }
        else if (Constant.STANDALONE.equalsIgnoreCase(mode)){
            redisClient = RedisReaderHelper.getJedis(addr, auth);
        }
        else {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,     String.format("无效的Redis模式:[%s]. 有效的模式为 'cluster' 或 'standalone'.", mode));
        }
        return redisClient;
    }

    public abstract void readData(RecordSender recordSender);

    public void close() {
        RedisReaderHelper.close(redisClient);
    }
}
