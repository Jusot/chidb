/*
 *  chidb - a didactic relational database management system
 *
 *  Miscellaneous functions
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
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "chidbInt.h"
#include "util.h"
#include "record.h"

/*
** Read or write a four-byte big-endian integer value.
* Based on SQLite code
*/
uint32_t get4byte(const uint8_t *p)
{
    return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}

void put4byte(unsigned char *p, uint32_t v)
{
    p[0] = (uint8_t)(v>>24);
    p[1] = (uint8_t)(v>>16);
    p[2] = (uint8_t)(v>>8);
    p[3] = (uint8_t)v;
}

int getVarint32(const uint8_t *p, uint32_t *v)
{
    *v = 0;

    *v  = ((uint32_t) (p[3] & 0x7F));
    *v |= ((uint32_t) (p[2] & 0x7F)) << 7;
    *v |= ((uint32_t) (p[1] & 0x7F)) << 14;
    *v |= ((uint32_t) (p[0] & 0x7F)) << 21;

    return CHIDB_OK;
}

int putVarint32(uint8_t *p, uint32_t v)
{
    uint32_t aux;

    aux = v & 0x0000007F;
    aux |= ((v & 0x00003F80) << 1) | 0x00008000;
    aux |= ((v & 0x001FC000) << 2) | 0x00800000;
    aux |= ((v & 0x0FE00000) << 3) | 0x80000000;

    p[3] = (uint8_t)(aux);
    p[2] = (uint8_t)(aux>>8);
    p[1] = (uint8_t)(aux>>16);
    p[0] = (uint8_t)(aux>>24);

    return CHIDB_OK;
}


void chidb_BTree_recordPrinter(BTreeNode *btn, BTreeCell *btc)
{
    DBRecord *dbr;

    chidb_DBRecord_unpack(&dbr, btc->fields.tableLeaf.data);

    printf("< %5i >", btc->key);
    chidb_DBRecord_print(dbr);
    printf("\n");

    free(dbr);
}

void chidb_BTree_stringPrinter(BTreeNode *btn, BTreeCell *btc)
{
    printf("%5i -> %10s\n", btc->key, btc->fields.tableLeaf.data);
}

int chidb_astrcat(char **dst, char *src)
{
    *dst = realloc(*dst, strlen(*dst) + strlen(src) + sizeof(char));
    if (!*dst)
        return CHIDB_ENOMEM;
    strcat(*dst, src);
    return CHIDB_OK;
}


int chidb_Btree_print(BTree *bt, npage_t npage, fBTreeCellPrinter printer, bool verbose)
{
    BTreeNode *btn;

    chidb_Btree_getNodeByPage(bt, npage, &btn);

    if (btn->type == PGTYPE_TABLE_LEAF)
    {
        if (verbose)
            printf("Leaf node (page %i)\n", btn->page->npage);
        for(int i = 0; i<btn->n_cells; i++)
        {
            BTreeCell btc;

            chidb_Btree_getCell(btn, i, &btc);
            printer(btn, &btc);
        }
    }
    else if (btn->type == PGTYPE_TABLE_INTERNAL)
    {
    	chidb_key_t last_key;

        if(verbose)
            printf("Internal node (page %i)\n", btn->page->npage);
        for(int i = 0; i<btn->n_cells; i++)
        {
            BTreeCell btc;

            chidb_Btree_getCell(btn, i, &btc);

            last_key = btc.key;
            if(verbose)
                printf("Printing Keys <= %i\n", last_key);
            chidb_Btree_print(bt, btc.fields.tableInternal.child_page, printer, verbose);
        }
        if(verbose)
            printf("Printing Keys > %i\n", last_key);
        chidb_Btree_print(bt, btn->right_page, printer, verbose);
    }
    else if (btn->type == PGTYPE_INDEX_LEAF)
    {
        if (verbose)
            printf("Leaf node (page %i)\n", btn->page->npage);
        for(int i = 0; i<btn->n_cells; i++)
        {
            BTreeCell btc;

            chidb_Btree_getCell(btn, i, &btc);
            printf("%10i -> %10i\n", btc.key, btc.fields.indexLeaf.keyPk);
        }
    }
    else if (btn->type == PGTYPE_INDEX_INTERNAL)
    {
    	chidb_key_t last_key;

        if(verbose)
            printf("Internal node (page %i)\n", btn->page->npage);
        for(int i = 0; i<btn->n_cells; i++)
        {
            BTreeCell btc;

            chidb_Btree_getCell(btn, i, &btc);
            last_key = btc.key;
            if(verbose)
                printf("Printing Keys < %i\n", last_key);
            chidb_Btree_print(bt, btc.fields.indexInternal.child_page, printer, verbose);
            printf("%10i -> %10i\n", btc.key, btc.fields.indexInternal.keyPk);
        }
        if(verbose)
            printf("Printing Keys > %i\n", last_key);
        chidb_Btree_print(bt, btn->right_page, printer, verbose);
    }

    chidb_Btree_freeMemNode(bt, btn);

    return CHIDB_OK;
}

FILE *copy(const char *from, const char *to)
{
    FILE *fromf, *tof;
    char ch;

    if( (fromf = fopen(from, "rb")) == NULL || (tof = fopen(to, "wb")) == NULL)
        return NULL;

    /* copy the file */
    while(!feof(fromf))
    {
        ch = fgetc(fromf);
        if(ferror(fromf))
            return NULL;
        fputc(ch, tof);
        if(ferror(tof))
            return NULL;
    }

    if(fclose(fromf)==EOF || fclose(tof)==EOF)
        return NULL;

    return tof;
}


int chidb_tokenize(char *str, char ***tokens)
{
    char *s;
    int ntokens = 0;

    s = str;
    if (s==NULL)
        return CHIDB_ENOMEM;

    /* First pass: Add \0 at the end of each token
     * and count the number of tokens */
    while(isspace(*s)) s++;

    while(*s != '\0')
    {
        ntokens++;
        if (*s == '"')
        {
            s++;
            while(*s && *s != '"') s++;
        }
        else
            while(*s && !isspace(*s)) s++;

        if(*s != '\0')
        {
            *s++ = '\0';
            while(*s && isspace(*s)) s++;
        }
    }


    /* Second pass: Create the array of tokens */
    *tokens = malloc(sizeof(char**) * ntokens);

    s = str;
    while(isspace(*s)) s++;
    for(int i=0; i<ntokens; i++)
    {
        if (*s == '"')
        {
            s++;
            (*tokens)[i] = s;
            while(*s && *s != '"') s++;
        }
        else
        {
            (*tokens)[i] = s;
            while(*s && !isspace(*s)) s++;
        }

        s++;
        while(*s && isspace(*s)) s++;
    }

    return ntokens;
}

// --------- My Code Begin ---------

int chidb_check_table_exist(chidb_schema_t schema, char *table)
{
    // 初始化迭代器
    list_iterator_start(&schema);

    // 遍历schema每一项
    while (list_iterator_hasnext(&schema))
    {
        // 获取当前项的指针
        chidb_schema_item_t *item = (chidb_schema_item_t *)(list_iterator_next(&schema));
        // name 与 table相等则返回1
        if (!strcmp(item->name, table))
        {
            list_iterator_stop(&schema);
            return 1;
        }
    }

    // 结束迭代器
    list_iterator_stop(&schema);

    // 不存在返回0
    return 0;
}

int chidb_get_root_page_of_table(chidb_schema_t schema, char *table)
{
    // 初始化迭代器
    list_iterator_start(&schema);

    // 遍历schema每一项
    while (list_iterator_hasnext(&schema))
    {
        // 获取当前项的指针
        chidb_schema_item_t *item = (chidb_schema_item_t *)(list_iterator_next(&schema));
        // name 与 table相等则返回对应的root page
        if (!strcmp(item->name, table))
        {
            list_iterator_stop(&schema);
            return item->root_page;
        }
    }

    // 结束迭代器
    list_iterator_stop(&schema);

    // 不存在返回0
    return 0;
}

int chidb_check_column_exist(chidb_schema_t schema, char *table, char *column)
{
    // 初始化迭代器
    list_iterator_start(&schema);

    // 遍历schema每一项
    while (list_iterator_hasnext(&schema))
    {
        // 获取当前项的指针
        chidb_schema_item_t *item = list_iterator_next(&schema);
        // name 与 table相等则在其中查找
        if (!strcmp(item->name, table))
        {
            // 遍历给定表中的列
            Column_t *cur_column = item->stmt->stmt.create->table->columns;
            while (cur_column != NULL)
            {
                // 若当前列名与给定列名相同, 返回1
                if (!strcmp(cur_column->name, column))
                {
                    list_iterator_stop(&schema);
                    return 1;
                }
                // 若不同则更新当前列指针指向下一列
                cur_column = cur_column->next;
            }
            // 不存在返回0
            list_iterator_stop(&schema);
            return 0;
        }
    }

    list_iterator_stop(&schema);
    // 不存在返回0
    return 0;
}

int chidb_get_type_of_column(chidb_schema_t schema, char *table, char *column)
{
    // 初始化迭代器
    list_iterator_start(&schema);

    // 遍历schema每一项
    while (list_iterator_hasnext(&schema))
    {
        // 获取当前项的指针
        chidb_schema_item_t *item = (chidb_schema_item_t *)(list_iterator_next(&schema));
        // name 与 table相等则在其中查找
        if (!strcmp(item->name, table))
        {
            // 遍历给定表中的列
            Column_t *cur_column = item->stmt->stmt.create->table->columns;
            while (cur_column != NULL)
            {
                // 若当前列名与给定列名相同, 返回类型
                if (!strcmp(cur_column->name, column))
                {
                    list_iterator_stop(&schema);
                    return cur_column->type;
                }
                // 若不同则更新当前列指针指向下一列
                cur_column = cur_column->next;
            }
            // 不存在返回-1
            list_iterator_stop(&schema);
            return -1;
        }
    }

    list_iterator_stop(&schema);
    // 不存在返回-1
    return -1;
}

int chidb_get_columns_of_table(chidb_schema_t schema, char *table, list_t columns)
{
    // 初始化迭代器
    list_iterator_start(&schema);

    // 遍历schema每一项
    while (list_iterator_hasnext(&schema))
    {
        // 获取当前项的指针
        chidb_schema_item_t *item = (chidb_schema_item_t *)(list_iterator_next(&schema));
        // name 与 table相等则获取其中的列后返回
        if (!strcmp(item->name, table))
        {
            // 遍历列, 添加到列名列表中
            Column_t *column = item->stmt->stmt.create->table->columns;
            while (column != NULL)
            {
                list_append(&columns, column);
                column = column->next;
            }

            list_iterator_stop(&schema);
            return CHIDB_OK;
        }
    }

    // 结束迭代器
    list_iterator_stop(&schema);
    // 不存在返回错误
    return CHIDB_EINVALIDSQL;
}

void chisql_statement_free(chisql_statement_t *sql_stmt)
{
    switch (sql_stmt->type)
    {
        case STMT_CREATE:
        {
            Create_free(sql_stmt->stmt.create);
        } break;

        case STMT_SELECT:
        {
            SRA_free(sql_stmt->stmt.select);
        } break;

        case STMT_INSERT:
        {
            Insert_free(sql_stmt->stmt.insert);
        } break;

        case STMT_DELETE:
        {
            Delete_free(sql_stmt->stmt.delete);
        } break;
    }

    free(sql_stmt->text);
    free(sql_stmt);
}
// --------- My Code End ---------
