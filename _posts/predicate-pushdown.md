---
layout: post
title: "Pushing Down Predicates in Apache Kudu"
author: Andrew Wong
---

I had the pleasure of interning with the Apache Kudu team this summer. I am
extremely thankful for all of the mentoring and support I received, and that I
got to be a part of Kudu’s journey from incubating to Top Level Apache project
to, soon enough, General Availability. This project was my summer contribution
to Kudu: a restructuring of the scan path to speed up queries.

<!--more-->

## A Day in the Life of a Query
Because Kudu is a columnar storage engine, its scan path has a number of
optimizations to avoid extraneous reads, copies, and computation. When a query
is sent to a tablet server, the server prunes tablets based on the
primary key, directing the request to only the tablets that contain the key
range of interest. Once at a tablet, only the columns relevant to the query are
scanned. Further pruning is be done over the primary key, and if the query is
predicated on non-key columns, the entire column is scanned. The columns in a
tablet are stored as _cfiles_, which are split into encoded _blocks_. Once the
relevant cfiles are determined, the data are materialized by the block
decoders, i.e. their underlying data are decoded and copied into a buffer,
which is passed back to the tablet layer. The tablet can then evaluate the
predicate on the batch of data and mark which rows should be returned to the
client.

One of the encoding types I worked very closely with is _dictionary encoding_,
an encoding type for strings that performs particularly well for columns that
have repeating values. Rather than storing every row’s string, each unique
string is assigned a numeric _codeword_, and the column is stored numerically
on disk. When materializing a dictionary block, all of the numeric data are
scanned and all of the corresponding strings are buffered for evaluation. When
the vocabulary of a dictionary-encoded block gets too large, Kudu will
automatically switch to _plain encoding_.

In plain-encoded blocks, strings are stored back-to-back, and the character
offsets to the start of each word are stored as a list of integers. When
materializing, all of the strings are copied to a buffer for evaluation.

Therein lies room for improvement: this predicate evaluation path is the same
for all data types and encoding types. Within the tablet, the correct cfiles
are determined, the cfiles’ decoders are opened, all of the data are copied to
a buffer, and the predicate is evaluated on this buffered data via
type-specific comparators. This path is extremely flexible, but because it was
designed to be encoding-independent, there is room for improvements.

## Trimming the Fat
The first step is to allow the decoders access to the predicate. In doing so,
each encoding type can specialize its evaluation. Additionally, this puts the
decoder in a position where it can determine whether a given row satisfies the
query, which in turn, allows the decoders to determine what data gets copied
instead of eagerly copying all of its data to get evaluated.

Take the case of dictionary-encoded strings as an example. With the existing
scan path, not only are all of the strings in a column copied into a buffer,
but string comparisons are done on every row. By taking advantage of the fact
that the data can be represented as integers, the cost of determining the query
results can be greatly amortized. The string comparisons can be swapped out
with evaluation based on the codewords, in which case the room for improvement
then boils down to how to most quickly determine whether or not a given
codeword corresponds to a string that satisfies the predicate. Kudu will now
use a bitset to store the matching codewords. When the decoder evaluates a
predicate, it scans through the integer-valued data and checks the bitset to
determine whether it should copy the corresponding string over.

This is great in the best case scenario where a column’s dictionary is small,
but when there are too many unique values to fit in a dictionary, performance
suffers. The plain decoders don’t utilize any dictionary metadata and ends up
wasting the codeword bitset. That isn’t to say all is lost: plain decoders can
still evaluate a predicate via string comparison, and the fact that evaluation
can still occur at the decoder-level means the eager buffering can still be
avoided.

Dictionary encoding is a perfect storm in that the decoders can completely
evaluate the predicates. This is not the case for most other encoding types,
but having decoders support evaluation leaves the door open for other encoding
types to extend this idea.

## Performance
Depending on the dataset and query, predicate pushdown can lead to significant
improvements. Tablet scans were timed with datasets consisting of repeated
strings of tunable length and tunable cardinality. 

![png]({{ site.github.url }}/img/predicate-pushdown/pushdown-10.png)
![png]({{ site.github.url }}/img/predicate-pushdown/pushdown-10M.png)

The above tablet scan times were recorded using a dataset of ten million rows of
strings with length ten. Predicates were designed to select values out of bounds
(Empty), select a single value (Equal, i.e. for cardinality _k_, this would
select one _k_th of the dataset), select half of the full range (Half), and
select the full range of values (All).

With the original evaluation implementation, the tablet must still copy and scan
through the tablet to determine whether any values match. This means that even
when the result set is small, the full column is still copied. This can be seen
in examining the above queries, as those with near-empty result sets (Empty and
Equal) have shorter scan times than those with larger result sets (Half and
Full).

Note that for dictiony encoding, given a low cardinality, Kudu can completely
rely on the dictionary codewords to evaluate, making the query significantly
faster. At higher cardinalities, the dictionaries completely fill up and the
blocks fall back on plain encoding. The slower, albeit still improved, the
performance on the dataset containing 10M unique values reflects this.

![png]({{ site.github.url }}/img/predicate-pushdown/pushdown-tpch.png)

Similar predicates were run with the TPC-H dataset, querying on the shipdate
column. The full path of a query consists not only of the scanning itself, but
also RPCs and data transfer to the caller. Regardless, signficant improvements
on the scan path still yield substantial improvements to the query as a whole.

## Conclusion
Pushing down predicates in Kudu yielded substantial improvements to the scan
path. For dictionary encoding, pushdown can be particularly powerful, depending
on the dataset, and other encoding types are either uneffected or also improved.

This summer has been a phenomenal learning experience for me, in terms of the
tools, the workflow, the datasets, the thought-processes that go into building
something at Kudu’s scale. I can’t express enough how grateful I am for the
amount of support I got from the Kudu team, from the intern coordinators, and
from the Cloudera community as a whole.
