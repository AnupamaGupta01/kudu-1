# Predicate Evaluation Pushdown
## READ Path
In order to internalize the concept of predicate evaluation pushdown, it is important to understand what is being pushed where. To contextualize these terms, the processes mentioned here occur on a single tablet. This means that pruning has already occurred (if possible), and that it was deemed necessary to search this tablet. Things begin in the tablet service, where a new scan request is received. This request is used to create a new iterator object that iterates over and fetches rows. Between this row-wise iterator and the block decoder (which houses the underlying data), are a number of other iterators, namely a column-wise iterator that selects a single column, and a column iterator, that iterates over a column.
```
        +----------------------+
        | TabletServiceImpl::  | tablet_service.h
        | HandleNewScanRequest |
        +----------+-----------+
                   |
                   |iter->NextBlock(&block)
                   |
     +-------------v--------------+
     |     iter:RowwiseIter       | iterator.h
     |     (Tablet::Iterator)     | tablet.h
     +----------------------------+
     | UnionIterator,DeltaApplier |
     |  other wrapping iterators  |
     | +------------------------- |
     | |     RowwiseIterator    | | iterator.h
     | |(MaterializingIterator) | | generic_iterators.h
     | |                        | |
     | | +--------------------+ | |
     | | | ColumnwiseIterator | | | iterator.h
     | | |(CFileSet::Iterator)| | | cfile_set.h
     | | | +----------------+ | | |
     | | | | ColumnIterator | | | |
     | | | | (CFileIterator)| | | | cfile_reader.h
     | | | +--------+-------+ | | |
     | | +--------| | |-------+ | |
     | +----------| | |---------+ |
     +------------+ | +-----------+
                    |
            +-------v-------+
            | PreparedBlock |  cfile_reader.h
            +-------+-------+
                    |
                    |
              +-----v------+
              |BlockDecoder|
              +------------+
```
Once the iterator is created with the scan request, it gets called to pull data from the underlying decoder. Once the data is returned to the MaterializingIterator level, the scan's predicates are evaluated, comparing each value against the bounds specified by the scans. The evaluation entails dispatching the correct comparator, specific to the column type (string, int, etc.).
```
         +---------------+
   Next  |               |
   Block | Materializing |
    ---> |    Iterator   |
         |               |
         +-------+-------+            +------------+
                 |      Materialize   | CFileSet:: |   Scan     +---------------+
                 +------Column------> |  Iterator  +----------> | CFileIterator |
                 |                    +------------+            +-------+-------+
                 |                                                      | CopyNextValues
                 |                                                      v
                 |                                              +-------+-------+
                 |           Decoded column is returned         | PreparedBlock |
                 +<---------------------------------------------+               |
                 |                                              |  BlockDecoder |
                 |              Apply                           +---------------+
                 |              Predicate           +-------------------+
                 |         +-----------------+      |    +----------+   |
                 |         |                 |  Compare  |  Column  |   v
                 +-------->+ ColumnPredicate +------+--> | TypeInfo |   v
                 Evaluate  |                 |      |    |          |   |
                           +-----------------+      ^    +----------+   |
                                                    ^                   |
                                                    |           for nrows
                                                    +-------------------+
```
It is also important to note that the evaluation is merely the setting and clearing of bits in a bitmap that indicates whether or not the row at a given index is to be returned as a part of the results set. The materialization of the filtered dataset occurs afterwards in the tablet service right before returning.

The premise of this project is to avoid this excessive use of CPU by evaluating the predicates as close to the block decoder as possible. In certain cases, evaluation or partial evaluation can be achieved much more efficiently. For instance, in the case of dictionary encoding, predicates can be evaluated using codeword (integer) comparison, rather than string comparison. In cases where this level of evaluation is not possible (i.e. if the decoder does not support any means of evaluation), the scan can fall back on separate materialization and evaluation.

## At the Block Decoder
### BinaryDictBlock
The important thing to understand about a BinaryDictBlock is that it is made up of two blocks: a dictionary block that contains all of the unique words in the order they were added (generally a BinaryPlainBlock) and a data block that stores the rows of the column in the form of their codewords (type UINT32). When materializing the data for a predicate evaluation, the existing dictionary blocks decode their data so the predicate may be evaluated on the strings individually (in a layer separate from the dictionary block).
- Overall performance:
    - `n: number of rows`
    - `d: number of distinct words`
    - `l: average length of word`
    - Evaluation: `O(n * l)`
- Note: In all implementations, the entire column is decoded and copied. The issue being tackled here is not the cost of disk access, but the computation required to select the correct rows.

### Evaluating the Predicate

The first step in evaluating a predicate at the decoder is determining the codewords that satisfy the predicate (specified by an upper and lower bound). Given the existing dictionary decoder, the most straightforward implementation is to iterate through the dictionary entries, comparing each one to the bounds. Those that satisfy the predicate are stored in a set, and the the data are evaluated by checking whether each word's codeword is in the set.
- Pros: this implementation is straightforward and does not require much alteration to the code. Assuming the cardinality of the column is low, i.e. the dictionary contains only a few words, this should perform better than the original implementation
- Cons: for columns with higher cardinality, the performance can take a huge hit from searching the set of satisfying predicates (this could be `O(log d)` for ordered sets or `O(C)` for unordered sets).
- Overall performance: 
    - Ordered set: `O(d log d * l + n * log d)`
    - Unordered set: `O(d * C1 * l + n * C2)`
- Link to implementation

Another approach is to use a sorted ordering of the codewords based on their decoded strings. With a sorted dictionary, the upper and lower bounds can be determined in `O(log d)` time and predicate evaluation again avoids string comparison in favor of integer comparison.
- Pros: sorting can be done at write-time, leaving minimal work to be done at read-time (i.e. finding the codewords nearest to the predicate bounds). This method also benefits from lower cardinality
- Cons: slightly more complicated write-time
- Overall performance:
    - Finding bounds: `O(log d * l)` + Evaluation: `O(n)`
- Link to implementation

