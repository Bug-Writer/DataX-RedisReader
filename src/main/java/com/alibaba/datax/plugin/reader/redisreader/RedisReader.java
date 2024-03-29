package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RedisReader extends Reader {
    private static final Logger LOG = LoggerFactory.getLogger(RedisReader.class);

    public static class Job extends Reader.Job {

        private Configuration config = null;

        @Override
        public void init() {
            this.config = super.getPluginJobConf();
            RedisReaderHelper.checkConnection(config);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> splitConfigs = new ArrayList<>();
            for (int i = 0; i < /*adviceNumber*/ 1; ++i) {
                splitConfigs.add(config.clone()); // [TODO] 完善分割逻辑
            }
            return splitConfigs;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {
        private Configuration taskConfig;
        private RedisReadAbstract reader;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.reader = new StringTypeReader(taskConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            this.reader.readData(recordSender);
        }

        @Override
        public void destroy() {
            this.reader.close();
        }
    }
}
