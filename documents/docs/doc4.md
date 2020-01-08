#

## 4. 查询优化

### 4.1 优化条件

将选取操作尽量后移, 实现的查询优化仅针对以下情况:

即当查询语句的 SRA 形如

```
Project([*],
    Select(t.a > int 10,
        NaturalJoin(
            Table(t),
            Table(u)
        )
    )
)
```

即查询语句格式为 `SELECT (columns or *) from table1 |><| table2 where table1.column op value;` 时, 可将得到的 SRA 优化为如下结构:

```
Project([*],
    NaturalJoin(
        Select(t.a > int 10,
                Table(t)
        ),
        Table(u)
    )
)
```

即将先自然连接后选取优化为先进行选取操作后再进行自然连接操作

### 4.2 条件检查

判断解析 SQL 语句得到的 SRA 结构是否满足优化条件,
若满足则进行选取操作后移优化, 不满足则直接复制原 SRA 结构

按照以下流程进行条件检查:

[配图]

### 4.3 选取后移 (Sigma Push)

1. 获取原 SRA 中 Select 中条件中的左边值所处的列引用
2. 获取原 SRA 中的自然连接中的两个表引用
3. 与列引用中相同的表名记为 table1, 另一个记为 table2
4. 为优化后的 SRA 及成员分配空间
5. 新 SRA 中的自然连接的右边设置为 Table 类型的 SRA, 其连接的表名为 table2
6. 新 SRA 中的 Select 部分的条件与原 SRA 中的条件相同, Select 中的 Table 连接的表名为 table1
