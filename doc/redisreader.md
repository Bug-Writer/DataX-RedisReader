# RedisReader 插件文档

___

## 1 快速介绍

RedisReader 插件实现了从 Redis 读取数据的功能。在底层实现上，RedisReader 通过 Jedis 客户端连接 Redis 实例，并执行相应的命令以从 Redis 中读取数据。

## 2 实现原理

简而言之，RedisReader 通过 Jedis 客户端连接到 Redis 实例，并根据用户配置的信息（例如键名、读取模式等）读取数据。然后，它将读取的数据使用 DataX 自定义的数据类型封装为抽象的数据集，并传递给下游 Writer 处理。

## 3 功能说明

### 3.1 配置样例

* 配置一个从 Redis 同步抽取数据到流处理的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "redisreader",
                    "parameter": {
                        "address": "127.0.0.1:6379",
                        "auth": "",
                        "include": "user1",
                        "mode": "standalone"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

* **address**

    * 描述：Redis 实例的地址，格式为 `host:port`
    * 必选：是
    * 默认值：无

* **auth**

    * 描述：Redis 连接的认证密码
    * 必选：否 
    * 默认值：无（空字符串）

* **include**

    * 描述：指定要读取的 Redis 键
    * 必选：是
    * 默认值：无

* **mode**

    * 描述：指定 Redis 运行模式，可以是 `standalone`（单机模式）或 `cluster`（集群模式）
    * 必选：是
    * 默认值：无

## 4 性能报告

RedisReader 的性能主要取决于 Redis 实例的响应速度和网络条件。在良好的网络环境下，RedisReader 能够快速读取并传输数据。

## 5 约束限制

* 目前 RedisReader 支持读取单个键的值。如果需要读取多个键或使用复杂的查询，可能需要自定义扩展。

* 读取性能受 Redis 实例性能和网络状况影响。

## 6 FAQ

**Q: 如何处理 Redis 的不同数据类型？**

A: RedisReader 目前主要支持读取字符串类型的键值。对于其他类型如列表、集合等，可能需要自定义实现。

**Q: 如果 Redis 实例需要密码认证，该如何配置？**

A: 在 `parameter` 部分中设置 `auth` 参数为你的 Redis 密码即可。如果不需要密码，可以省略此参数或留空。

**Q: 如何在集群模式下使用 RedisReader？**

A: 将 `mode` 参数设置为 `cluster`，并确保 `address` 参数指向正确的 Redis 集群地址。
