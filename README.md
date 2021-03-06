chidb - A didactic RDBMS
=====================================

The chidb documentation is available at http://chi.cs.uchicago.edu/

## Quick Start

### Preparation

```
./autogen.sh
./configure
make
```

### Run

```
./chidb
```

### Check

```
make check
```

### Quick Usage

```
./chidb
chidb> .open test.cdb
chidb> CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER);
chidb> INSERT INTO products VALUES(1, "Hard Drive", 240);
chidb> SELECT * FROM products;
```

## Need to Complete

### Assignment 1

#### Files

+ [x] src/libchidb/btree.c

#### Steps

+ [x] Step 1: Opening a chidb file
+ [x] Step 2: Loading a B-Tree node from the file
+ [x] Step 3: Creating and writing a B-Tree node to disk
+ [x] Step 4: Manipulating B-Tree cells
+ [x] Step 5: Finding a value in a B-Tree
+ [x] Step 6: Insertion into a leaf without splitting
+ [x] Step 7: Insertion with splitting
+ [x] Step 8: Supporting index B-Trees

### Assignment 2

+ [x] src/libchidb/dbm.c
+ [x] src/libchidb/dbm-ops.c
+ [x] src/libchidb/dbm-cursor.\[hc\]

### Assignment 3

#### Files

+ [x] src/libchidb/chidbInt.h
+ [x] src/libchidb/api.c
+ [x] src/libchidb/utils.\[hc\]
+ [x] src/libchidb/codegen.c

#### Steps

+ [x] Step 1: Schema Loading
+ [x] Step 2: Simple SELECT Code Generation
+ [x] Step 3: INSERT Code Generation
+ [x] Step 4: CREATE TABLE Code Generation
+ [ ] Step 5: NATURAL JOIN Code Generation

### Assignment 4

+ [x] src/libchidb/optimizer.c
