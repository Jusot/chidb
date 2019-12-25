/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <chidb/log.h>
#include <sys/stat.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"

// 文件头中的常量
uint8_t header_between_18_23[6] = { 0x01, 0x01, 0x00, 0x40, 0x20, 0x20 },
        header_between_32_39[8] = { 0 },
        header_between_44_47[4] = { 0, 0, 0, 0x01 },
        header_between_52_59[8] = { 0, 0, 0, 0, 0, 0, 0, 0x01 },
        header_between_64_67[4] = { 0 };

/* Open a B-Tree file
 *
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 *
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    Pager *pager;

    // 尝试为bt分配空间
    if (!(*bt = malloc(sizeof(Btree))))
    {
        // 初始化失败返回无可用内存错误
        return CHIDB_ENOMEM;
    }

    // 尝试读取文件
    int status = chidb_Pager_open(&pager, filename);
    if (status != CHIDB_OK)
    {
        // 若失败则返回错误码
        return status;
    }

    // 将相对应的成员互相绑定
    (*bt)->pager = pager;
    (*bt)->db = db;
    db->bt = *bt;

    struct stat file_stat;
    // 读取文件的信息
    fstat(fileno(pager->f), &file_stat);

    // 若文件为空
    if (file_stat.st_size == 0)
    {
        // 1) 通过默认页大小初始化文件头
        chidb_Pager_setPageSize(pager, DEFAULT_PAGE_SIZE);
        pager->n_pages = 0;

        // 2) 在页1中创建一个空的表页结点
        npage_t npage;
        if ((status = chidb_Btree_newNode(*bt, &npage, PGTYPE_TABLE_LEAF)) != CHIDB_OK)
        {
            // 创建页结点失败则返回对应的错误码
            return status;
        }
    }
    // 若文件非空
    else
    {
        // 文件头规定了前一百个字节
        uint8_t buf[100];

        // 尝试读取文件头
        if ((status = chidb_Pager_readHeader(pager, buf)) != CHIDB_OK)
        {
            // 文件头读取失败则返回相应的错误码
            return status;
        }

        // 验证文件头中的相关常量
        if (!memcmp(buf, "SQLite format 3", 16)
         && !memcmp(&buf[18], header_between_18_23, 6)
         && !memcmp(&buf[32], header_between_32_39, 8)
         && !memcmp(&buf[44], header_between_44_47, 4)
         && !memcmp(&buf[52], header_between_52_59, 8)
         && !memcmp(&buf[64], header_between_64_67, 4)
         && get4byte(&buf[48]) == 20000)
        {
            // 常量都正确则读取Page size并设置pager的page_size成员
            // Page size 在文件头0x12即16的位置
            chidb_Pager_setPageSize(pager, get2byte(&buf[16]));
        }
        // 验证失败, 返回表示错误文件头的验证码
        else
        {
            return CHIDB_ECORRUPTHEADER;
        }
    }

    return CHIDB_OK;
}


/* Close a B-Tree file
 *
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 *
 * Parameters
 * - bt: B-Tree file to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
    // 尝试关闭指向的页文件
    int status = chidb_Pager_close(bt->pager);
    if (status != CHIDB_OK)
    {
        // 如果关闭失败返回CHIDB_EIO
        return status;
    }

    // 释放bt指向的空间
    free(bt);

    return CHIDB_OK;
}


/* Loads a B-Tree node from disk
 *
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    // 尝试分配结点空间
    if (!(*btn = malloc(sizeof(BTreeNode))))
    {
        // 分配失败返回错误码
        return CHIDB_ENOMEM;
    }

    // 尝试读取npage指定页的内容到结点中页指针成员指向的内存中
    int status = chidb_Pager_readPage(bt->pager, npage, &(*btn)->page);
    if (status != CHIDB_OK)
    {
        // 读取失败返回错误码
        return status;
    }

    // 如果读取的是第一页, 则需要加上文件头(100字节)的偏移量
    // 否则不添加偏移量
    uint8_t *data = (*btn)->page->data + ((npage == 1) ? 100 : 0);

    // 按照格式初始化结点中的成员
    BTreeNode *node = *btn;
    // 第一个字节为Page type
    node->type = *data;
    // 字节1-2为表示可用空间开始的字节偏移量Free offset
    node->free_offset = get2byte(data + 1);
    // 字节3-4为此页面中存储的单元格数
    node->n_cells = get2byte(data + 3);
    // 字节5-5为单元格开始处的字节偏移量
    node->cells_offset = get2byte(data + 5);
    // 当页面类型为内部表页面或者内部索引页面时, 该值才有意义
    node->right_page = ((node->type == 0x05) || (node->type == 0x02)) ? get4byte(data + 8) : 0;
    // 单元格偏移数组存储在页面头之后的位置
    node->celloffset_array = data + (((node->type == 0x05) || (node->type == 0x02)) ? 12 : 8);

    return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 *
 * Frees the memory allocated to an in-memory B-Tree node, and
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    // 尝试释放内存中的页
    int status = chidb_Pager_releaseMemPage(bt->pager, btn->page);
    if (status != CHIDB_OK)
    {
        // 释放失败返回错误码
        return status;
    }

    // 释放接收的B树结点所指向的空间
    free(btn);

    return CHIDB_OK;
}


/* Create a new B-Tree node
 *
 * Allocates a new page in the file and initializes it as a B-Tree node.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    // 尝试分配新页
    int status = chidb_Pager_allocatePage(bt->pager, npage);
    if (status == CHIDB_OK)
    {
        // 分配成功则初始化空结点
        status = chidb_Btree_initEmptyNode(bt, *npage, type);
    }

    // 返回对应的返回码
    return status;
}


/* Initialize a B-Tree node
 *
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    MemPage *page;

    // 尝试读取页到内存中
    int status = chidb_Pager_readPage(bt->pager, npage, &page);
    if (status != CHIDB_OK)
    {
        // 读取失败返回错误码
        return status;
    }

    uint8_t *pos = page->data;

    // 如果是第一页, 需要写入文件头
    if (npage == 1)
    {
        sprintf(pos, "SQLite format 3");
        pos += 16;

        // 写入Page size
        put2byte(pos, bt->pager->page_size);
        pos += 2;

        // 写入字节12-23的常量
        memcpy(pos, header_between_18_23, 6);
        pos += 6;

        // 写入文件修改次数, 初始化为0, +8是因为有4字节是未使用的
        put4byte(pos, 0);
        pos += 8;

        // 写入字节32-39的常量
        memcpy(pos, header_between_32_39, 8);
        pos += 8;

        // 初始化版本为0
        put4byte(pos, 0);
        pos += 4;

        // 写入字节44-47的常量
        memcpy(pos, header_between_44_47, 4);
        pos += 4;

        // 写入页缓存大小
        put4byte(pos, 20000);
        pos += 4;

        // 写入字节52-59的常量
        memcpy(pos, header_between_52_59, 8);
        pos += 8;

        // 写入User Cookie
        put4byte(pos, 0);
        pos += 4;

        // 写入字节64-67的常量
        put4byte(pos, 0);

        pos = page->data + 100;
    }

    // 写入页头
    // 写入页类型
    *pos++ = type;

    // 写入可用空间的偏移量
    // 假设可用空间从头部之后的定量偏移
    // 内部结点的定量偏移为12
    // 非内部结点则为8
    put2byte(pos,
        ((type == 0x05) || (type == 0x02) ? 12 : 8)
        + ((npage == 1) ? 100 : 0)
    );
    pos += 2;

    // 写入单元格偏移
    put2byte(pos, bt->pager->page_size);
    pos += 2;

    *pos++ = 0;

    // 若为内部结点则写入Right page
    if (type == 0x05 || type == 0x02)
    {
        put4byte(pos, 0);
        pos += 4;
    }

    // 尝试写入操作, 并返回结果
    // 若成功则返回CHIDB_OK
    // 否则返回对应的错误码
    return chidb_Pager_writePage(bt->pager, page);
}



/* Write an in-memory B-Tree node to disk
 *
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    // 若当前页为第一页则偏移为100
    uint8_t *pos = ((btn->page->npage == 1) ? 100 : 0) + btn->page->data;

    // 按照格式写入数据
    *pos = btn->type;
    put2byte(pos + 1, btn->free_offset);
    put2byte(pos + 3, btn->n_cells);
    put2byte(pos + 5, btn->cells_offset);
    if (btn->type == 0x05
     || btn->type == 0x02)
    {
        put4byte(pos + 8, btn->right_page);
    }

    // 返回写入页的结果
    return chidb_Pager_writePage(bt->pager, btn->page);
}


/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
 *
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    // 当前单元格的数据
    uint8_t *data = btn->page->data + get2byte(btn->celloffset_array + ncell * 2);

    if (ncell < 0 || ncell > btn->n_cells)
    {
        // ncell非法则返回错误
        return CHIDB_ECELLNO;
    }

    // 单元格类型与结点类型相同
    cell->type = btn->type;
    switch (btn->type)
    {
    case PGTYPE_TABLE_INTERNAL:
        // 若为内部表单元
        // 字节0-3为ChildPage, 类型为uint32
        // 字节4-7为Key, 类型为varint32
        cell->fields.tableInternal.child_page = get4byte(data);
        getVarint32(data + 4, &cell->key);
        break;

    case PGTYPE_TABLE_LEAF:
        // 若为叶表单元格
        // 字节0-3为DB Record size, 类型为varint32
        // 字节4-7为Key, 类型为varint32
        // 字节8-后为DB Record
        getVarint32(data, &cell->fields.tableLeaf.data_size);
        getVarint32(data + 4, &cell->key);
        cell->fields.tableLeaf.data = data + TABLELEAFCELL_SIZE_WITHOUTDATA;
        break;

    case PGTYPE_INDEX_INTERNAL:
        // 若为内部索引单元
        // 字节0-3为ChildPage, 类型为uint32
        // 字节8-11为KeyIdx, 类型为uint32
        // 字节12-15为KeyPk, 类型为uint32
        cell->fields.indexInternal.child_page = get4byte(data);
        cell->key = get4byte(data + 8);
        cell->fields.indexInternal.keyPk = get4byte(data + 12);
        break;

    case PGTYPE_INDEX_LEAF:
        // 若为叶索引单元
        // 字节4-7为KeyIdx, 类型为uint32
        // 字节8-11为KeyPk, 类型为uint32
        cell->key = get4byte(data + 4);
        cell->fields.indexLeaf.keyPk = get4byte(data + 8);
        break;

    default:
        fprintf(stderr, "getCell: invalid page type (%d)\n", btn->type);
        exit(1);
    }

    return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    /* Your code goes here */

    return CHIDB_OK;
}

/* Find an entry in a table B-Tree
 *
 * Finds the data associated for a given key in a table B-Tree
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    /* Your code goes here */

    return CHIDB_OK;
}



/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    // 声明单元格结构成员后插入B树中

    BTreeCell btc;
    btc.type = PGTYPE_TABLE_LEAF;
    btc.key = key;
    btc.fields.tableLeaf.data_size = size;
    btc.fields.tableLeaf.data = data;

    return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    BTreeCell btc;
    btc.type = PGTYPE_INDEX_LEAF;
    btc.key = keyIdx;
    btc.fields.indexLeaf.keyPk = keyPk;

    return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    /* Your code goes here */

    return CHIDB_OK;
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    /* Your code goes here */

    return CHIDB_OK;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    /* Your code goes here */

    return CHIDB_OK;
}
