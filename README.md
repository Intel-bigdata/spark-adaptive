# Spark SQL Adaptive Execution

There are three main features in Adaptive Execution, including auto setting the shuffle partition number, optimizing join strategy at runtime and handling skewed join. These features can be enabled separately. To start with Adaptive Exection, please build branch AdaptiveJoin3 and at least set `spark.sql.adaptive.enabled` to true.

An Engilish version design doc is available on [google doc](https://docs.google.com/document/d/1mpVjvQZRAkD-Ggy6-hcjXtBPiQoVbZGe3dLnAKgtJ4k/edit). A Chinese version blog is available on [CSDN](https://mp.weixin.qq.com/s?__biz=MzA4Mzc0NjkwNA==&mid=2650784030&idx=1&sn=2c61e166b535199ee53e579a5092ff80&chksm=87faa829b08d213f55dab289bf5a12cfe376be0c944e03279a1c93e0f0d2164f1c6a6c7c880a&mpshare=1&scene=1&srcid=0111fEEzMCuhKozD4hsN4EE5&pass_ticket=WwOAQGxxBX9z63UyuFIXnWVm%2FSJhHkYwdsKplVDbaiA66ueqnDOtzgq86NgTgqvt#rd) that introduces the features and benchmark results. [SPARK-23128](https://issues.apache.org/jira/browse/SPARK-23128) is the Jira for contributing this work to Apache Spark.


## Auto Setting The Shuffle Partition Number
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.enabled</code></td>
  <td>false</td>
  <td>
    When true, enable adaptive query execution.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.minNumPostShufflePartitions</code></td>
  <td>1</td>
  <td>
    The minimum number of post-shuffle partitions used in adaptive execution. This can be used to control the minimum parallelism.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.maxNumPostShufflePartitions</code></td>
  <td>500</td>
  <td>
    The maximum number of post-shuffle partitions used in adaptive execution. This is also used as the initial shuffle partition number so please set it to an reasonable value.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleInputSize</code></td>
  <td>67108864</td>
  <td>
    The target post-shuffle input size in bytes of a task. By default is 64 MB.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleRowCount</code></td>
  <td>20000000</td>
  <td>
    The target post-shuffle row count of a task. This only takes effect if row count information is collected.
  </td>
</tr>
</table>

## Optimizing Join Strategy at Runtime
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.join.enabled</code></td>
  <td>true</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a better join strategy is determined at runtime.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptiveBroadcastJoinThreshold</code></td>
  <td>equals to <code>spark.sql.autoBroadcastJoinThreshold</code></td>
  <td>
    Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join in adaptive exeuction mode. If not set, it equals to <code>spark.sql.autoBroadcastJoinThreshold</code>.
  </td>
</tr>
</table>

## Handling Skewed Join
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.skewedJoin.enabled</code></td>
  <td>false</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a skewed join is automatically handled at runtime.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionFactor</code></td>
  <td>10</code></td>
  <td>
    A partition is considered as a skewed partition if its size is larger than this factor multiple the median partition size and also larger than <code>spark.sql.adaptive.skewedPartitionSizeThreshold</code>, or if its row count is larger than this factor multiple the median row count and also larger than <code>spark.sql.adaptive.skewedPartitionRowCountThreshold</code>.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionSizeThreshold</code></td>
  <td>67108864</td>
  <td>
    Configures the minimum size in bytes for a partition that is considered as a skewed partition in adaptive skewed join.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionRowCountThreshold</code></td>
  <td>10000000</td>
  <td>
    Configures the minimum row count for a partition that is considered as a skewed partition in adaptive skewed join.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.statistics.verbose</code></td>
  <td>false</td>
  <td>
    Collect shuffle statistics in verbose mode, including row counts etc. This is required for handling skewed join.
  </td>
</tr>
</table>
