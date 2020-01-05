/*
 *  chidb - a didactic relational database management system
 *
 *  Miscellaneous functions and definitions
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

#ifndef UTIL_H_
#define UTIL_H_

#include "chidbInt.h"
#include "btree.h"
#include <chidb/utils.h>

/*
** Read or write a two- and four-byte big-endian integer values.
* Based on SQLite code
*/
#define get2byte(x)   ((x)[0]<<8 | (x)[1])
#define put2byte(p,v) ((p)[0] = (uint8_t)((v)>>8), (p)[1] = (uint8_t)(v))

/* Return the distance in bytes between the pointers elm and hd */
#define OFFSET(hd, elm) ((uint8_t *)(&(elm))-(uint8_t *)(&hd))

uint32_t get4byte(const uint8_t *p);
void put4byte(unsigned char *p, uint32_t v);
int getVarint32(const uint8_t *p, uint32_t *v);
int putVarint32(uint8_t *p, uint32_t v);

int chidb_astrcat(char **dst, char *src);

typedef void (*fBTreeCellPrinter)(BTreeNode *, BTreeCell*);
int chidb_Btree_print(BTree *bt, npage_t nroot, fBTreeCellPrinter printer, bool verbose);
void chidb_BTree_recordPrinter(BTreeNode *btn, BTreeCell *btc);
void chidb_BTree_stringPrinter(BTreeNode *btn, BTreeCell *btc);

FILE *copy(const char *from, const char *to);

// --------- My Code Begin ---------
// ------------------------------------
/** In step 1, functions to perform the following operations
 * 1. Given a table name, determine whether such a table exists.
 * 2. Given a table name, obtain its root page.
 * 3. Given a table name and a column name, determine whether such a column exists in the table.
 * 4. Given a table name and a column name, obtain the type of the column.
 */
// 检查是否已存在给定table, 存在则返回1, 不存在则返回0
int chidb_check_table_exist(chidb_schema_t schema, char *table);
// 获取给定table所在的根页码, 不存在则返回0
int chidb_get_root_page_of_table(chidb_schema_t schema, char *table);
// 检查在给定table中是否存在给定列, 存在返回1, 否则返回0
int chidb_check_column_exist(chidb_schema_t schema, char *table, char *column);
// 获取给定table中给定column的类型, 不存在则返回-1
int chidb_get_type_of_column(chidb_schema_t schema, char *table, char *column);

// 根据给定的表名获取其所有的列
int chidb_get_columns_of_table(chidb_schema_t schema, char *table, list_t columns);

void chisql_statement_free(chisql_statement_t *sql_stmt);
// --------- My Code End ---------
#endif /*UTIL_H_*/
