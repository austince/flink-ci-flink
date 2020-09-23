---
title: "文件系统 SQL 连接器"
nav-title: FileSystem
nav-parent_id: sql-connectors
nav-pos: 5
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

该连接器提供了对 Flink 支持的文件系统中的分区文件的访问，Flink 支持的文件系统参阅 [Flink FileSystem abstraction]({{ site.baseurl}}/ops/filesystems/index.html).

* This will be replaced by the TOC
{:toc}

FileSystem 连接器本身就被包括在 Flink 中，不需要任何额外的依赖。
当从文件系统中读取或向文件系统写入记录时，需要指定相应的记录格式。

FileSystem 连接器支持对本地文件系统或分布式文件系统的读取和写入。 可以通过如下方式定义 FileSystem 表:

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- 必填: 指定连接器类型
  'path' = 'file:///path/to/whatever',  -- 必填：指定目录的路径
  'format' = '...',                     -- 必填: FileSystem 连接器需要指定格式，
                                        -- 请参阅 表格式 
                                        -- 章节以获得更多详情
  'partition.default-name' = '...',     -- 可选: 当动态分区列的值是 null 或空字符串时，
                                        -- 默认的分区名
  
  -- 可选: 该参数控制 sink 阶段是否根据动态分区列 shuffle 数据， 开启该功能能大大
  -- 减少 filesystem sink 的文件数量，但可能会造成数据倾斜，默认值是 false.
  'sink.shuffle-by-partition.enable' = '...',
  ...
)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">注意</span> 需要确保包含以下依赖 [Flink File System specific dependencies]({{ site.baseurl }}/ops/filesystems/index.html).

<span class="label label-danger">注意</span> 针对流的 FileSystem sources 目前还在开发中。 将来，社区会不断添加对常见的流处理场景的支持, 比如对分区和目录的检测等。

<span class="label label-danger">注意</span> 新版的 FileSystem 连接器和旧版的 FileSystem 连接器有很大不同：path 参数指定的是一个目录而不是一个文件，该目录下文件的格式也不是肉眼可读的。

## 分区文件

Flink 的 FileSystem 连接器在对分区的支持上，使用了标准的 hive 格式。 不过，它不需要预先注册分区，而是基于目录结构自动做了分区发现。比如，以下目录结构的表， 会被自动推导为包含 `datetime` 和 `hour` 分区的分区表。

```
path
└── datetime=2019-08-25
    └── hour=11
        ├── part-0.parquet
        ├── part-1.parquet
    └── hour=12
        ├── part-0.parquet
└── datetime=2019-08-26
    └── hour=6
        ├── part-0.parquet
```

FileSystem 连接器支持分区新增插入和分区覆盖插入。 参见 [INSERT Statement]({{ site.baseurl }}/dev/table/sql/insert.html). 当对分区表进行分区覆盖插入时，只有相应的分区会被覆盖，而不是整个表。

## 文件格式

FileSystem 连接器支持多种格式：

 - CSV: [RFC-4180](https://tools.ietf.org/html/rfc4180). 非压缩格式。
 - JSON: FileSystem 连接器中的 JSON 不是传统的标准的 JSON 格式，而是非压缩的 [newline delimited JSON](http://jsonlines.org/).
 - Avro: [Apache Avro](http://avro.apache.org). 可以通过配置 `avro.codec` 支持压缩。
 - Parquet: [Apache Parquet](http://parquet.apache.org). 与 Hive 兼容。
 - Orc: [Apache Orc](http://orc.apache.org). 与 Hive 兼容。

## 流式 Sink

FileSystem 连接器支持流式的写, 它基于 Flink 的 [Streaming File Sink]({{ site.baseurl }}/dev/connectors/streamfile_sink.html)
将记录写入文件。按行编码的格式支持 csv 和 json。 按块编码的格式支持 parquet, orc 和 avro。

你可以直接编写 SQL，把流数据插入到非分区表。
如果是分区表，你可以配置分区操作相关的参数，参加 [Partition Commit](filesystem.html#partition-commit) 以查阅更多细节。

### 滚动策略

分区目录下的数据被分割到分区文件中。分区对应的sink的每个子任务都至少会生成一个分区文件。 当前分区文件的关闭和新的分区文件的生成，是根据配置的滚动策略来的。 可以配置基于文件大小，或时间来滚动分区文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.rolling-policy.file-size</h5></td>
        <td style="word-wrap: break-word;">128MB</td>
        <td>MemorySize</td>
        <td> 分区文件滚动前的最大大小.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td> 分区文件滚动前，保持打开状态的最大时长 (为避免生成过多的小文件，默认是30分钟).
        该配置项的检测频率是由以下参数控制的 'sink.rolling-policy.check-interval' .</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td style="word-wrap: break-word;">1 min</td>
        <td>Duration</td>
        <td> 基于时间的滚动策略的检测时间间隔。该参数控制了检测分区文件基于参数 'sink.rolling-policy.rollover-interval' 是否应该发生滚动的检测频率。</td>
    </tr>
  </tbody>
</table>

**注意:** 对于 bulk 格式 (parquet, orc, avro), 滚动策略和检查间隔控制了分区文件的大小和个数。

**注意:** 对于行格式 (csv, json), 如果想使得分区文件更快地在文件系统中可见，可以设置连接器参数 `sink.rolling-policy.file-size` 或 `sink.rolling-policy.rollover-interval` 和 flink-conf.yaml 中参数 `execution.checkpointing.interval` ；对于其他格式 (avro, orc), 则只需要配置 flink-conf.yaml 中的参数 `execution.checkpointing.interval`.

### 分区提交

分区数据写完毕后，经常需要通知下游应用。比如，在 Hive metastore 中新增分区 或者在目录下新增 `_SUCCESS` 文件。 分区提交策略是可定制的，具体的分区提交行为基于 `triggers` 和 `policies` 的组合. 

- Trigger: 分区提交的时机，可以基于从分区中提取的时间对应的水印，或者处理时间。
- Policy: 分区提交策略，内置的策略包括 `_SUCCESS` 文件 和 hive metastore， 也可以自己定制提交策略, 比如触发 hive 生成统计信息，合并小文件等。

**注意:** 分区提交只有在动态分区插入模式下才有效。

#### 分区提交的触发器

通过配置分区提交的触发策略，来配置何时提交分区：

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.trigger</h5></td>
        <td style="word-wrap: break-word;">process-time</td>
        <td>String</td>
        <td>分区提交触发器的类型: 'process-time': 基于机器时间，该触发器既不需要分区时间提取也不需要水印生成。一旦 '当前系统时间' 超过了 '分区创建的系统时间' 和 'delay' 的和，就提交分区。 'partition-time': 基于分区字段值提取的时间, 该触发器需要生成水印。一旦水印超过 '从分区字段值提取的时间' 和 'delay' 的和，就提交分区。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td> 分区不会在该延迟时间前提交。 如果是按天的分区，应该配置为 '1 d', 如果是按小时的分区，应该配置为 '1 h'.</td>
    </tr>
  </tbody>
</table>

有两种类型的触发器：
- 第一种是根据分区的处理时间。 该触发器不需要分区时间提取，也不需要生成水印。通过分区创建时间和当前系统时间来触发分区提交，更通用但不是很精确。比如，数据的延迟或故障转移都会导致分区的提前提交。
- 第二种是根据分区字段值提取的时间以及水印。这需要你的作业支持生成水印，分区是根据时间来切割的，比如按小时或按天。

如果想让下游系统尽快感知到分区，而不管分区数据是否完整：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='0s' (默认值)
一旦分区中有数据，分区立马就会被提交。注意：分区可能会被多次提交。

如果想让下游系统只有在分区数据完整时才感知到分区，且你的作业有水印生成的逻辑，也能从分区字段的值中提取到时间：
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1h' (按小时生成分区)
该方式是最精确的提交分区的方式，该方式尽力确保提交的分区包含尽量完整的数据。

如果想让下游系统只有在数据完整时才感知到分区，但是没有水印，或者无法从分区字段的值中提取时间：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='1h' (按小时生成分区)
该方式尽量精确地提交分区，但是数据延迟或故障转移会导致分区的提前提交。

延迟数据的处理：延迟的记录会被写入到已经提交的对应分区中，且会再次触发该分区的提交。

#### 分区时间的提取器

时间提取器定义了如何从分区字段值中提取时间。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>partition.time-extractor.kind</h5></td>
        <td style="word-wrap: break-word;">default</td>
        <td>String</td>
        <td>从分区字段值中提取时间的时间提取器。支持默认和定制。对于默认的，可以配置时间戳格式；对于定制的，需要配置提取器对应的类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>实现了 PartitionTimeExtractor 接口的时间提取类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td> 默认的时间提取器允许用户从分区字段中提取一个合法的时间戳格式。 默认支持第一个分区字段的 'yyyy-mm-dd hh:mm:ss' 时间戳格式。如果要从一个分区字段 'dt'提取时间，可以配置为: '$dt'。 如果需要从多个分区字段提取时间戳，比如 'year', 'month', 'day' and 'hour', 可以配置为: '$year-$month-$day $hour:00:00'. 如果需要从两个分区字段比如 'dt' 和 'hour' 提取时间戳，可以配置为: '$dt $hour:00:00'.</td>
    </tr>
  </tbody>
</table>

默认的提取器是基于由分区字段组合而成的时间戳模式。你也可以指定一个实现了 `PartitionTimeExtractor` 接口的自定义的提取器. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

public class HourPartTimeExtractor implements PartitionTimeExtractor {
    @Override
    public LocalDateTime extract(List<String> keys, List<String> values) {
        String dt = values.get(0);
        String hour = values.get(1);
		return Timestamp.valueOf(dt + " " + hour + ":00:00").toLocalDateTime();
	}
}

{% endhighlight %}
</div>
</div>

#### 分区提交策略

分区提交策略指定了提交分区时的具体操作。 

- 第一种是 metastore, 只有 hive 表支持该策略, 该策略下文件系统通过目录层次结构来管理分区。
- 第二种是 success 文件, 该策略下会在分区对应的目录下写入一个名为 `_SUCCESS` 的空文件.

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.policy.kind</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td> 分区提交策略指的是如何通知下游应用分区已经写完毕，可以被读取了。metastore: 在 metastore 中添加分区，只有 hive 表支持 metastore 策略， 此时文件系统是通过目录的层次结构来关系分区的； success-file: 在目录下添加 '_success' 文件； 可以同时指定两者: 'metastore,success-file'； custom: 使用自定义类作为分区提交策略.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>实现了 PartitionCommitPolicy 接口的分区提交策略类。只有在 custom 分区提交策略下有效。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td> success-file 分区提交策略时的使用的标志文件名，默认是 '_SUCCESS'.</td>
    </tr>
  </tbody>
</table>

你也可以实现自己的提交策略， 如：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

public class AnalysisCommitPolicy implements PartitionCommitPolicy {
    private HiveShell hiveShell;
	@Override
	public void commit(Context context) throws Exception {
	    if (hiveShell == null) {
	        hiveShell = createHiveShell(context.catalogName());
	    }
	    hiveShell.execute(String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s = '%s') location '%s'",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0),
	        context.partitionPath()));
	    hiveShell.execute(String.format(
	        "ANALYZE TABLE %s PARTITION (%s = '%s') COMPUTE STATISTICS FOR COLUMNS",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0)));
	}
}

{% endhighlight %}
</div>
</div>

## 完整示例

如下示例演示了，如何使用文件系统连接器编写流查询语句查询kafka中的数据并写入到文件系统中，以及通过批查询把结果数据读取出来。 

{% highlight sql %}

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  log_ts TIMESTAMP(3),
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
) WITH (...);

CREATE TABLE fs_table (
  user_id STRING,
  order_amount DOUBLE,
  dt STRING,
  hour STRING
) PARTITIONED BY (dt, hour) WITH (
  'connector'='filesystem',
  'path'='...',
  'format'='parquet',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.policy.kind'='success-file'
);

-- 流 sql, 插入到文件系统表中
INSERT INTO TABLE fs_table SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table;

-- 批 sql, 通过分区裁剪来查询数据
SELECT * FROM fs_table WHERE dt='2020-05-20' and hour='12';

{% endhighlight %}

{% top %}
