/*
 *  chidb - a didactic relational database management system
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file.
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


#include <stdlib.h>
#include <chidb/chidb.h>
#include "dbm.h"
#include "btree.h"
#include "record.h"
#include "util.h"
#include "../simclist/simclist.h"

/* Implemented in codegen.c */
int chidb_stmt_codegen(chidb_stmt *stmt, chisql_statement_t *sql_stmt);

/* Implemented in optimizer.c */
int chidb_stmt_optimize(chidb *db,
			chisql_statement_t *sql_stmt,
			chisql_statement_t **sql_stmt_opt);

// --------- My Code Begin ---------

// Step 1
// 读取Schema表
int load_schema(chidb *db, npage_t nroot)
{
	Btree *bt = db->bt;

	// 读取nroot页
	BTreeNode *btn;
	chidb_Btree_getNodeByPage(bt, nroot, &btn);

	// 遍历当前页面的所有cells
	int i;
	for (i = 0; i < btn->n_cells; ++i)
	{
		BTreeCell *cell = malloc(sizeof(BTreeCell));
		chidb_Btree_getCell(btn, i, cell);

		// 如果是页表内部结点
		if (btn->type == PGTYPE_TABLE_INTERNAL)
		{
			load_schema(db, cell->fields.tableInternal.child_page);
		}
		// 如果是页表叶子结点
		else if (btn->type == PGTYPE_TABLE_LEAF)
		{
			DBRecord *dbr;
			chidb_DBRecord_unpack(&dbr, cell->fields.tableLeaf.data);
			// 为schema中的一行申请空间
			chidb_schema_item_t *item = malloc(sizeof(chidb_schema_item_t));
			// 将Record中的字段写入schema
			chidb_DBRecord_getString(dbr, 0, &item->type);
			chidb_DBRecord_getString(dbr, 1, &item->name);
			chidb_DBRecord_getString(dbr, 2, &item->assoc);
			chidb_DBRecord_getInt32 (dbr, 3, &item->root_page);
			char *sql;
			chidb_DBRecord_getString(dbr, 4, &sql);
			// 解析sql语句写入schema->stmt
			chisql_parser(sql, &item->stmt);
			// 将该行加入到db中的schema里
			list_append(&db->schema, item);
			// 释放空间
			free(sql);
			chidb_DBRecord_destroy(dbr);
		}

		free(cell);
	}

	// 如果不是叶子结点, 则还需要加载right page
	if (btn->type != PGTYPE_TABLE_LEAF)
	{
		load_schema(db, btn->right_page);
	}

	// 释放内存
	chidb_Btree_freeMemNode(bt, btn);
	return CHIDB_OK;
}

int chidb_open(const char *file, chidb **db)
{
    *db = malloc(sizeof(chidb));
    if (*db == NULL)
        return CHIDB_ENOMEM;
    chidb_Btree_open(file, *db, &(*db)->bt);

	// 初始化schema
	list_init(&(*db)->schema);
	// 初始化need_refresh
	(*db)->need_refresh = 0;
	// 读取schema
	load_schema(*db, 1);

    return CHIDB_OK;
}

int chidb_close(chidb *db)
{
    chidb_Btree_close(db->bt);

	// 不断获取list中的第一个值并释放其中的指针指向的空间
	while (!list_empty(&db->schema))
	{
		chidb_schema_item_t *item = (chidb_schema_item_t *)list_fetch(&db->schema);
		chisql_statement_free(item->stmt);
		free(item->type);
		free(item->name);
		free(item->assoc);
		free(item);
	}
	// 释放list的空间
	list_destroy(&db->schema);

    free(db);
    return CHIDB_OK;
}

// --------- My Code End ---------

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
    int rc;
    chisql_statement_t *sql_stmt, *sql_stmt_opt;

    *stmt = malloc(sizeof(chidb_stmt));

    rc = chidb_stmt_init(*stmt, db);

    if(rc != CHIDB_OK)
    {
        free(*stmt);
        return rc;
    }

    rc = chisql_parser(sql, &sql_stmt);

    if(rc != CHIDB_OK)
    {
        free(*stmt);
        return rc;
    }

    rc = chidb_stmt_optimize((*stmt)->db, sql_stmt, &sql_stmt_opt);

    if(rc != CHIDB_OK)
    {
        free(*stmt);
        return rc;
    }

    rc = chidb_stmt_codegen(*stmt, sql_stmt_opt);

    free(sql_stmt_opt);

    (*stmt)->explain = sql_stmt->explain;

    return rc;
}

int chidb_step(chidb_stmt *stmt)
{
	if(stmt->explain)
	{
		if(stmt->pc == stmt->endOp)
			return CHIDB_DONE;
		else
		{
			stmt->pc++;
			return CHIDB_ROW;
		}
	}
	else
		return chidb_stmt_exec(stmt);
}

int chidb_finalize(chidb_stmt *stmt)
{
    return chidb_stmt_free(stmt);
}

int chidb_column_count(chidb_stmt *stmt)
{
	if(stmt->explain)
		return 6;
	else
		return stmt->nCols;
}

int chidb_column_type(chidb_stmt *stmt, int col)
{
	if(stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch(col)
		{
		case 0:
			return SQL_INTEGER_4BYTE;
		case 1:
			return 2 * strlen(opcode_to_str(op->opcode)) + SQL_TEXT;
		case 2:
		case 3:
		case 4:
			return SQL_INTEGER_4BYTE;
		case 5:
			if(op->p4 == NULL)
				return SQL_NULL;
			else
				return 2 * strlen(op->p4) + SQL_TEXT;
		default:
			return SQL_NOTVALID;
		}
	}
	else
	{
		if(col < 0 || col >= stmt->nCols)
			return SQL_NOTVALID;
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			switch(r->type)
			{
			case REG_UNSPECIFIED:
			case REG_BINARY:
				return SQL_NOTVALID;
				break;
			case REG_NULL:
				return SQL_NULL;
				break;
			case REG_INT32:
				return SQL_INTEGER_4BYTE;
				break;
			case REG_STRING:
				return 2 * strlen(r->value.s) + SQL_TEXT;
				break;
			default:
				return SQL_NOTVALID;
			}
		}
	}
}

const char *chidb_column_name(chidb_stmt* stmt, int col)
{
	if(stmt->explain)
	{
		switch(col)
		{
		case 0:
			return "addr";
		case 1:
			return "opcode";
		case 2:
			return "p1";
		case 3:
			return "p2";
		case 4:
			return "p3";
		case 5:
			return "p4";
		default:
			return NULL;
		}
	}
	else
	{
		if(col < 0 || col >= stmt->nCols)
			return NULL;
		else
			return stmt->cols[col];
	}
}

int chidb_column_int(chidb_stmt *stmt, int col)
{
	if(stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch(col)
		{
		case 0:
			return stmt->pc - 1;
		case 1:
			return 0; /* Undefined */
		case 2:
			return op->p1;
		case 3:
			return op->p2;
		case 4:
			return op->p3;
		case 5:
			return 0; /* Undefined */
		default:
			return 0; /* Undefined */
		}
	}
	else
	{
		if(col < 0 || col >= stmt->nCols)
		{
			/* Undefined behaviour */
			return 0;
		}
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			if(r->type != REG_INT32)
			{
				/* Undefined behaviour */
				return 0;
			}
			else
			{
				return r->value.i;
			}
		}
	}
}

const char *chidb_column_text(chidb_stmt *stmt, int col)
{
	if(stmt->explain)
	{
		chidb_dbm_op_t *op = &stmt->ops[stmt->pc - 1];

		switch(col)
		{
		case 0:
			return NULL; /* Undefined */
		case 1:
			return opcode_to_str(op->opcode);
		case 2:
			return NULL; /* Undefined */
		case 3:
			return NULL; /* Undefined */
		case 4:
			return NULL; /* Undefined */
		case 5:
			return op->p4;
		default:
			return 0; /* Undefined */
		}
	}
	else
	{
		if(col < 0 || col >= stmt->nCols)
		{
			/* Undefined behaviour */
			return NULL;
		}
		else
		{
			chidb_dbm_register_t *r = &stmt->reg[stmt->startRR + col];

			if(r->type != REG_STRING)
			{
				/* Undefined behaviour */
				return NULL;
			}
			else
			{
				return r->value.s;
			}
		}
	}
}
