/*
 *  chidb - a didactic relational database management system
 *
 *  Query Optimizer
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
#include "dbm-types.h"

// -- My Code Begin --

// 检查是否可以优化, 可以返回1, 否则返回0
int check_optionmizable(chisql_statement_t *sql_stmt);

// 从Select设置合适的Join
int set_naturaljoin(SRA_Binary_t *join_opt, SRA_Select_t *select);

int chidb_stmt_optimize(chidb *db, chisql_statement_t *sql_stmt, chisql_statement_t **sql_stmt_opt)
{
   // 为优化后的chisql_statement申请空间
    *sql_stmt_opt = malloc(sizeof(chisql_statement_t));

    // 如果不满足优化条件, 直接复制原来的sql返回
    if (!check_optionmizable(sql_stmt))
    {
        memcpy(*sql_stmt_opt, sql_stmt, sizeof(chisql_statement_t));
        return CHIDB_OK;
    }

    /* 可优化时, SRA形如
    Project([*],
        Select(t.a > int 10,
                NaturalJoin(
                        Table(t),
                        Table(u)
                )
        )
    )
    ->
    Project([*],
            NaturalJoin(
                    Select(t.a > int 10,
                            Table(t)
                    ),
                    Table(u)
            )
    )
    */
    SRA_Project_t *project = &sql_stmt->stmt.select->project;

    // 设置为select类型
    chisql_statement_t *stmt_opt = *sql_stmt_opt;
    stmt_opt->type = STMT_SELECT;
    // 复制project的expr_list部分
    stmt_opt->stmt.select = malloc(sizeof(SRA_t));
    // 设置select类型为SRA_PROJECT
    stmt_opt->stmt.select->t = SRA_PROJECT;
    SRA_Project_t *project_opt = &stmt_opt->stmt.select->project;
    project_opt->expr_list = malloc(sizeof(Expression_t));
    memcpy(project_opt->expr_list, project->expr_list, sizeof(Expression_t));

    // 为优化后的stmt生成narutaljoin
    project_opt->sra = malloc(sizeof(SRA_t));
    // 设置类型为连接
    project_opt->sra->t = SRA_NATURAL_JOIN;
    set_naturaljoin(&project_opt->sra->binary, &project->sra->select);

    return CHIDB_OK;
}

int check_optionmizable(chisql_statement_t *sql_stmt)
{
    // 当形如"SELECT * FROM t |><| u WHERE t.a>10;"时即可优化
    // 即SRA可进行如下优化
    /*
    Project([*],
        Select(t.a > int 10,
                NaturalJoin(
                        Table(t),
                        Table(u)
                )
        )
    )
    ->
    Project([*],
            NaturalJoin(
                    Select(t.a > int 10,
                            Table(t)
                    ),
                    Table(u)
            )
    )
    */

    if (sql_stmt->type != STMT_SELECT)
    {
        return 0;
    }

    SRA_Project_t *project = &sql_stmt->stmt.select->project;

    if (project->sra->t == SRA_SELECT)
    {
        SRA_Select_t *select = &project->sra->select;

        if (select->sra->t == SRA_NATURAL_JOIN)
        {
            return 1;
        }
    }

    return 0;
}

int set_naturaljoin(SRA_Binary_t *binary_opt, SRA_Select_t *select)
{
    SRA_Binary_t *binary = &select->sra->binary;

    // 记录原语句比较时出现的表
    ColumnReference_t *select_ref = select->cond->cond.comp.expr1->expr.term.ref;

    // 记录另自然连接中的另一个表
    TableReference_t *binary_ref = NULL;
    TableReference_t *ref1 = binary->sra1->table.ref,
                     *ref2 = binary->sra2->table.ref;
    binary_ref = (strcmp(select_ref->tableName, ref1->table_name) == 0)
               ? ref2 : ref1;


    // 设置连接的左边是select
    binary_opt->sra1 = malloc(sizeof(SRA_t));
    binary_opt->sra1->t = SRA_SELECT;
    SRA_Select_t *select_opt = &binary_opt->sra1->select;
    // 条件部分与之前相同
    select_opt->cond = malloc(sizeof(Condition_t));
    memcpy(select_opt->cond, select->cond, sizeof(Condition_t));
    // 设置select中的表
    select_opt->sra = malloc(sizeof(SRA_t));
    select_opt->sra->t = SRA_TABLE;
    select_opt->sra->table.ref = malloc(sizeof(TableReference_t));
    select_opt->sra->table.ref->table_name = select_ref->tableName;
    select_opt->sra->table.ref->alias = NULL;

    // 设置连接的右边是table
    binary_opt->sra2 = malloc(sizeof(SRA_t));
    // 设置table中的表
    binary_opt->sra2->t = SRA_TABLE;
    binary_opt->sra2->table.ref = binary_ref;

    return CHIDB_OK;
}

// -- My Code End --
