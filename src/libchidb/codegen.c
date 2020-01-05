/*
 *  chidb - a didactic relational database management system
 *
 *  SQL -> DBM Code Generator
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <chidb/chidb.h>
#include <chisql/chisql.h>
#include "dbm.h"
#include "util.h"

  /* ...code... */

// --------- My Code Begin ---------

// 创建一条Op指令
chidb_dbm_op_t *chidb_make_op(
    opcode_t opcode,
    int32_t p1, int32_t p2, int32_t p3,
    char *p4)
{
    chidb_dbm_op_t *op = malloc(sizeof(chidb_dbm_op_t));
    op->opcode = opcode;
    op->p1 = p1;
    op->p2 = p2;
    op->p3 = p3;
    op->p4 = p4;
    return op;
}

// Step 4
// 只包含创建表的代码生成
int chidb_create_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt, list_t *ops)
{
    Create_t *create = sql_stmt->stmt.create;
    char *text = sql_stmt->text;

    // 如果要创建的表名已存在则返回错误
    if (chidb_check_table_exist(stmt->db->schema, create->table->name))
    {
        return CHIDB_EINVALIDSQL;
    }

    // 在寄存器0上存储整数1
    list_append(ops, chidb_make_op(
        Op_Integer,
        1, // 存储整数1, 因为要打开schema, 所以该值为1
        0, // 在寄存器0上
        0, NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_OpenWrite,
        0, // 打开页码为寄存器0上存储的整数的页
        0, // 在游标0处存储
        5, // 列数为5, 因为是schema, 共有5列
        NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_CreateTable,
        4, // 新建一个表, 其B树所在页码存储在寄存器4上, 因为其值需要在记录中的第5列
        0, 0, NULL)); // not used

    // 按Schema记录的顺序存储值
    // 存储类型
    list_append(ops, chidb_make_op(
        Op_String,
        5, // 字符串长度为5
        1, // 存储在寄存器1上
        0, // not used
        "table")); // 类型为table

    // 存储名称
    list_append(ops, chidb_make_op(
        Op_String,
        strlen(create->table->name),
        2,
        0, // not used
        create->table->name));

    // 存储关联表名称, 因是创建table所以即是table的名称
    list_append(ops, chidb_make_op(
        Op_String,
        strlen(create->table->name),
        3,
        0, // not used
        create->table->name));

    // 存储sql语句
    list_append(ops, chidb_make_op(
        Op_String,
        strlen(text),
        5, // 在第五列
        0, // not used
        text));

    // 生成一行记录
    list_append(ops, chidb_make_op(
        Op_MakeRecord,
        1, // 从寄存器1开始
        5, // 遍历5个寄存器
        6, // 将5个寄存器的值生成一条记录存储在寄存器6中
        NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_Integer,
        list_size(&stmt->db->schema) + 1, // 其key即为在schema中的位置
        7, // 将key存储在寄存器7中
        0, NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_Insert,
        0, // 在游标0指向的B树中插入一条记录
        6, // 记录存储在寄存器6中
        7, // 以寄存器7中存储的值作为key
        NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_Close,
        0, // 关闭游标0指向的B树
        0, 0, NULL)); // not used

    return CHIDB_OK;
}

int chidb_select_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt, list_t *ops)
{
    return CHIDB_EINVALIDSQL;
}

// Step 3
// 完成insert语句的代码生成
int chidb_insert_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt, list_t *ops)
{
    Insert_t *insert = sql_stmt->stmt.insert;

    // 如果要插入的表不存在返回错误
    char *table_name = sql_stmt->stmt.insert->table_name;
    if (!chidb_check_table_exist(stmt->db->schema, table_name))
    {
        return CHIDB_EINVALIDSQL;
    }

    // 获取要插入的表的所有的列
    list_t columns;
    list_init(&columns);
    chidb_get_columns_of_table(stmt->db->schema, table_name, columns);

    // 遍历列, 检查每一列与对应的给定值类型是否匹配
    list_iterator_start(&columns);
    Literal_t *value = insert->values;
    while (list_iterator_hasnext(&columns))
    {
        Column_t *column = (Column_t *)(list_iterator_next(&columns));
        // 如果给定值的个数不符
        if (value == NULL)
        {
            list_destroy(&columns);
            return CHIDB_EINVALIDSQL;
        }
        // 如果当前值与对应列的类型不同, 返回错误
        if (value->t != column->type)
        {
            list_destroy(&columns);
            return CHIDB_EINVALIDSQL;
        }
    }
    list_iterator_stop(&columns);

    // 错误检查之后开始生成代码

    int root_page = chidb_get_root_page_of_table(stmt->db->schema, table_name);
    list_append(ops, chidb_make_op(
        Op_Integer,
        root_page, // 将要插入表所在的B树页码
        0, // 存储在寄存器0上
        0, NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_OpenWrite, // 以读写模式打开表所在的根B树
        0, // 游标0与之关联
        0, // 寄存器0存储要打开的B树页码
        list_size(&columns), // 要打开的表的列数
        NULL)); // not used

    // 生成一行记录
    // 遍历值, 不断存储在连续的寄存器上
    int reg = 1;
    value = insert->values;
    while (value != NULL)
    {
        // 根据不同的值类型生成不同的指令
        switch (value->t)
        {
        case TYPE_INT:
            list_append(ops, chidb_make_op(
                Op_Integer,
                value->val.ival,
                reg,
                0, NULL)); // not used
            break;
        case TYPE_TEXT:
            list_append(ops, chidb_make_op(
                Op_String,
                strlen(value->val.strval),
                reg,
                0, // not used
                value->val.strval));
            break;

        // 并没有Op_Double 和 Op_Char
        case TYPE_DOUBLE:
        case TYPE_CHAR:
            break;
        }
        // 第一个值总为主键, 要在值后标记NULL
        if (reg == 1)
        {
            list_append(ops, chidb_make_op(
                Op_Null,
                0, // not used
                ++reg,
                0, NULL)); // not used
        }
        ++reg;
        value = value->next;
    }

    list_append(ops, chidb_make_op(
        Op_MakeRecord,
        2, // 从寄存器2存储的值开始
        reg - 2, // 向后n个值的记录
        reg, // 存储在最后一个可用的寄存器上
        NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_Insert,
        0, // 将reg存储的记录插入到游标0关联的表上
        reg, // 指定记录所在的寄存器
        1, // 记录的key所在寄存器为1
        NULL)); // not used

    list_append(ops, chidb_make_op(
        Op_Close,
        0, // 关闭游标0关联的B树
        0, 0, NULL)); // not used

    list_destroy(&columns);
    return CHIDB_OK;
}

int chidb_stmt_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt)
{
    sql_stmt->text[strlen(sql_stmt->text) - 1] = '\0'; // 删除结尾的分号

    // 如果之前执行了create table的指令, 则需要重新load schema
    if (stmt->db->need_refresh == 1)
    {
        // 不断获取list中的第一个值并释放其中的指针指向的空间
        while (!list_empty(&stmt->db->schema))
        {
            chidb_schema_item_t *item = (chidb_schema_item_t *)list_fetch(&stmt->db->schema);
            chisql_statement_free(item->stmt);
            free(item->type);
            free(item->name);
            free(item->assoc);
            free(item);
        }
        // 释放list的空间
        list_destroy(&stmt->db->schema);
        // 重新初始化schema
        list_init(&stmt->db->schema);
        // 重新load schema
        int load_schema(chidb *db, npage_t nroot);
        load_schema(stmt->db, 1);
        // 更新 need_refresh 为0
        stmt->db->need_refresh = 0;
    }

    list_t ops;
    list_init(&ops);

    // 根据不同的语句调用不同的代码生成, 将指令添加到ops中
    int err = CHIDB_EINVALIDSQL;
    switch (sql_stmt->type)
    {
    case STMT_CREATE:
        err = chidb_create_codegen(stmt, sql_stmt, &ops);
        stmt->db->need_refresh = 1;
        break;

    case STMT_SELECT:
        err = chidb_select_codegen(stmt, sql_stmt, &ops);
        break;

    case STMT_INSERT:
        err = chidb_insert_codegen(stmt, sql_stmt, &ops);
        break;

    default:
        break;
    }

    if (err != CHIDB_OK)
    {
        // 释放空间
        while (!list_empty(&ops))
        {
            free(list_fetch(&ops));
        }
        list_destroy(&ops);
        return err;
    }

    stmt->sql = sql_stmt;
    stmt->nOps = list_size(&ops);

    // 添加指令到stmt中
    int i = 0;
    list_iterator_start(&ops);
    while (list_iterator_hasnext(&ops))
    {
        chidb_dbm_op_t *op = (chidb_dbm_op_t *)(list_iterator_next(&ops));
        chidb_stmt_set_op(stmt, op, i++);
        free(op);
    }
    list_iterator_stop(&ops);

    // 释放空间
    list_destroy(&ops);

// --------- My Code End ---------
    /*
    int opnum = 0;
    int nOps;
    */

    /* Manually load a program that just produces five result rows, with
     * three columns: an integer identifier, the SQL query (text), and NULL. */

    /* stmt->nCols = 3;
    stmt->cols = malloc(sizeof(char *) * stmt->nCols);
    stmt->cols[0] = strdup("id");
    stmt->cols[1] = strdup("sql");
    stmt->cols[2] = strdup("null");

    chidb_dbm_op_t ops[] = {
            {Op_Integer, 1, 0, 0, NULL},
            {Op_String, strlen(sql_stmt->text), 1, 0, sql_stmt->text},
            {Op_Null, 0, 2, 0, NULL},
            {Op_ResultRow, 0, 3, 0, NULL},
            {Op_Integer, 2, 0, 0, NULL},
            {Op_ResultRow, 0, 3, 0, NULL},
            {Op_Integer, 3, 0, 0, NULL},
            {Op_ResultRow, 0, 3, 0, NULL},
            {Op_Integer, 4, 0, 0, NULL},
            {Op_ResultRow, 0, 3, 0, NULL},
            {Op_Integer, 5, 0, 0, NULL},
            {Op_ResultRow, 0, 3, 0, NULL},
            {Op_Halt, 0, 0, 0, NULL},
    };

    nOps = sizeof(ops) / sizeof(chidb_dbm_op_t);

    for(int i=0; i < nOps; i++)
        chidb_stmt_set_op(stmt, &ops[i], opnum++);
 */
    return CHIDB_OK;

}
