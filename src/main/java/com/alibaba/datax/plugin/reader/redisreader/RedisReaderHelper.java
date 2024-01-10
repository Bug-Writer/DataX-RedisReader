package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

public class RedisReaderHelper {

    public static void checkConnection(Configuration userConfig) {
        String mode = userConfig.getNecessaryValue(Key.MODE, CommonErrorCode.CONFIG_ERROR);
        String addr = userConfig.getNecessaryValue(Key.ADDRESS, CommonErrorCode.CONFIG_ERROR);
        String auth = userConfig.getString(Key.AUTH);

        if (Constant.CLUSTER.equalsIgnoreCase(mode)) {
            JedisCluster jedisCluster = getJedisCluster(addr, auth);
            jedisCluster.set("testConnect", "test");
            jedisCluster.expire("testConnect", 1);
            jedisCluster.close();
        }
        else if (Constant.STANDALONE.equalsIgnoreCase(mode)) {
            Jedis jedis = getJedis(addr, auth);
            jedis.set("testConnect", "test");
            jedis.expire("testConnect", 1);
            jedis.close();
        }
        else {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
                    String.format("您提供配置文件有误，redis的mode必须是standalone或cluster，而[%s]为非法输入.", mode));
        }
    }

    public static Jedis getJedis(String addr, String auth) {
        String[] split = addr.split(":");
        Jedis jedis = new Jedis(split[0], Integer.parseInt(split[1]));
        if(StringUtils.isNoneBlank(auth)){
            jedis.auth(auth);
        }
        return jedis;
    }

    public static JedisCluster getJedisCluster(String addr, String auth) {
        final GenericObjectPoolConfig jedisPoolConfig = new JedisPoolConfig();
        JedisCluster jedisCluster;
        Set<HostAndPort> nodes = new HashSet<>();
        String[] split = addr.split(",");
        for (String node : split) {
            String[] hostPort = node.split(":");
            nodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        if (StringUtils.isBlank(auth)) {
            jedisCluster = new JedisCluster(nodes, 3000, 3000, 3, jedisPoolConfig);
        }
        else {
            jedisCluster = new JedisCluster(nodes, 3000, 3000, 3, auth, jedisPoolConfig);
        }

        return jedisCluster;
    }

    public static void close(Object obj) {
        if (obj instanceof Jedis) {
            ((Jedis) obj).close();
        }
        else if (obj instanceof JedisCluster) {
            ((JedisCluster) obj).close();
        }
    }
}
