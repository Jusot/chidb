#

## 代码生成

完成以下功能:

1. 实现读取 Schema 表
2. 实现简单 Select 语句到 DBM 指令的代码生成
3. 实现简单 Insert 语句到 DBM 指令的代码生成
4. 实现 Create Table 语句到 DBM 指令的代码生成

### 例子

```sql
CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER);
```

翻译为

```plain
Integer      1  0  _  _
OpenWrite    0  0  5  _

CreateTable  4  _  _  _

String       5  1  _  "table"
String       8  2  _  "products"
String       8  3  _  "products"
String       73 5  _  "CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER)"

MakeRecord   1  5  6  _
Integer      1  7  _  _

Insert       0  6  7  _

Close        0  _  _  _
```

### 数据流图

SQL 语句 -> 词法分析器 -> 语法分析器 -> 代码生成模块 -> DBM 指令

其中词法分析器和语法分析器由 chidb 提供

### 具体实现

#### 1. 读取 Schema 表

##### 1.1 定义 Schema Item 类型

在 chidbInt.h 中定义 chidb_schema_item_t 类型, 结构定义如下

```c
typedef struct
{
    char *type;
    char *name;
    char *assoc;
    int root_page;
    chisql_statement_t *stmt;
} chidb_schema_item_t;
```

其包含了 Schema 表中每一行的记录信息

##### 1.2 修改 chidb 结构体

在 chidbInt.h 中修改 struct chidb 的定义

从

```c
struct chidb
{
    BTree   *bt;
};
```

修改到

```c

```

#### 2. 简单 Select 语句的代码生成

#### 3. 简单 Insert 语句的代码生成

#### 4. Create Table 语句的代码生成
