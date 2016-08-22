# Performance for Decoder-Level Evaluation

Note: the experiments and plots were performed and generated with:
* 50 samples each
* the x-axis denoting the string lengths
* repeating values of specified cardinality (e.g. cardinality 4: [0,1,2,3,0,1,2,3,0,1,2,3])

# Dictionary Encoding

## High cardinality

![Performance 1](https://raw.githubusercontent.com/anjuwong/kudu/565e2c4e56f57ca738fcec73054d9297b9f72084/docs/images/decoder_eval_perf_1.png)
Left: High cardinality, low selectivity (all strings returned); Right: high cardinality, high selectivity (1/1M strings returned)

### High cardinality, low selectivity

Performance improvement is relatively minimal, as a high cardinality will lead to a large amount of the data being stored with plain encoding. Since the entire dataset is being returned, there is no improvement from fewer memcpys. Thus, the performance is roughly identical to that of normal evaluation, if not slightly worse due to preprocessing done for each cfile’s dictionary.

### High cardinality, high selectivity

For queries with higher selectivity, fewer results are returned, and we can thusly see a performance boost over normal evaluation. In the above query, almost no results are returned, and as such, particularly when the strings are long, fewer strings are copied from the decoder, resulting in a performance boost.

![Performance 1.5](https://raw.githubusercontent.com/anjuwong/kudu/565e2c4e56f57ca738fcec73054d9297b9f72084/docs/images/decoder_eval_perf_1.5.png)

Note that because the cells are being evaluated individually rather than in batches, the predicate comparator is determined once per cell instead of once per batch. This led to a fairly significant dip in performance, as the comparator had been determined completely dynamically. See the above plot and compare it against the plot above it on the right and note that the slower speed. To alleviate this, the evaluation has been templatized to remediate the cost of the repeated comparator dispatch.

## Low cardinality

![Performance 2](https://raw.githubusercontent.com/anjuwong/kudu/565e2c4e56f57ca738fcec73054d9297b9f72084/docs/images/decoder_eval_perf_2.png)
### Low cardinality, low selectivity

Dictionary encoded blocks perform much faster when the vocabulary is small. In this instance, even though the entire dataset is being copied over, there is still a performance spike from the minimal number of string comparisons.

### Low cardinality, high selectivity

As with the previous case, the performance is much better when the decoder evaluates the block. Only a few results are copied, resulting in an even greater speed-up.

# Plain Encoding
Overall, slightly worse performance is observed when it comes to queries with low selectivity, but generally, not that much worse than not using decoder-level evaluation. The biggest factor for speed-ups comes from the selectivity, since, given a highly selective query, there is greater potential to avoid a large number of memcpy operations.

## Low selectivity
![Performance 3](https://raw.githubusercontent.com/anjuwong/kudu/565e2c4e56f57ca738fcec73054d9297b9f72084/docs/images/decoder_eval_perf_3.png)
Left: high cardinality, Right: low cardinality, all strings selected

## High selectivity
![Performance 4](hatps://raw.githubusercontent.com/anjuwong/kudu/565e2c4e56f57ca738fcec73054d9297b9f72084/docs/images/decoder_eval_perf_4.png)
Left: high cardinality (1/1M strings selected), Right: low cardinality (1/100 strings selected)

