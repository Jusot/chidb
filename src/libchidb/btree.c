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

// --------- My Code Begin ---------

#define CHECK if (status != CHIDB_OK) return status

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
        sprintf((char *)pos, "SQLite format 3");
        pos += 16;

        // 写入Page size
        put2byte(pos, bt->pager->page_size);
        pos += 2;

        // 写入字节18-23的常量
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
    *(pos++) = type;

    // 写入可用空间的偏移量
    // 假设可用空间从头部之后的定量偏移
    // 内部结点的定量偏移为12
    // 非内部结点则为8
    put2byte(pos,
        ((type == 0x05) || (type == 0x02) ? 12 : 8)
        + ((npage == 1) ? 100 : 0)
    );
    pos += 2;

    // 写入Cells个数
    put2byte(pos, 0);
    pos += 2;

    // 写入单元格偏移
    put2byte(pos, bt->pager->page_size);
    pos += 2;

    *(pos++) = 0;

    // 若为内部结点则写入Right page
    if (type == 0x05 || type == 0x02)
    {
        put4byte(pos, 0);
        pos += 4;
    }

    // 尝试写入操作, 并返回结果
    // 若成功则返回CHIDB_OK
    if ((status = chidb_Pager_writePage(bt->pager, page)) == CHIDB_OK)
    {
        chidb_Pager_releaseMemPage(bt->pager, page);
    }

    // 返回对应的错误码
    return status;
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
    // 将cell插入到ncell在btn中指定的位置中

    uint8_t *data = btn->page->data;
    uint8_t *cell_pointer = NULL;
    uint8_t const_bytes[] = { 0x0B, 0x03, 0x04, 0x04 };

    // 如果ncell小于零或者大于btn指向的结点中的单元个数
    if(ncell < 0 || ncell > btn->n_cells)
    {
        // 返回错误码
        return CHIDB_ECELLNO;
    }

    switch(btn->type)
    {
        case PGTYPE_TABLE_LEAF:
            // 按照叶表单元格的格式存储, 从free space中得到其存储空间的起始地址
            // cells向下增长
            cell_pointer = data + btn->cells_offset - cell->fields.tableLeaf.data_size - TABLELEAFCELL_SIZE_WITHOUTDATA;
            // 字节0-3存储DB Record Size字段的值
            putVarint32(cell_pointer, cell->fields.tableLeaf.data_size);
            // 字节4-7存储Key字段
            putVarint32(cell_pointer + 4, cell->key);
            // 字节8-存储单元格的数据
            memcpy(cell_pointer + 8, cell->fields.tableLeaf.data, cell->fields.tableLeaf.data_size);
            // 更新btn的单元格偏移量
            btn->cells_offset -= (cell->fields.tableLeaf.data_size + TABLELEAFCELL_SIZE_WITHOUTDATA);
            break;

        case PGTYPE_TABLE_INTERNAL:
            // 按照内部表格单元的格式存储, 从free space中得到其存储空间的起始地址
            cell_pointer = data + btn->cells_offset - TABLEINTCELL_SIZE;
            // 字节0-3存储Child Page字段
            put4byte(cell_pointer, cell->fields.tableInternal.child_page);
            // 字节4-7存储Key字段
            putVarint32(cell_pointer + 4, cell->key);
            // 更新btn的单元格偏移量
            btn->cells_offset -= TABLEINTCELL_SIZE;
            break;

        case PGTYPE_INDEX_INTERNAL:
            // 按照内部索引的格式存储, 从free space中得到其存储空间的起始地址
            cell_pointer = data + btn->cells_offset - INDEXINTCELL_SIZE;
            // 字节0-3存储Child Page字段
            put4byte(cell_pointer, cell->fields.indexInternal.child_page);
            // 字节4-7存储固定字节
            memcpy(cell_pointer + 4, const_bytes, 4);
            // 字节8-11存储Key字段
            put4byte(cell_pointer + 8, cell->key);
            // 字节12-15存储KeyPK字段
            put4byte(cell_pointer + 12, cell->fields.indexInternal.keyPk);
            // 更新btn的单元格偏移量
            btn->cells_offset -= INDEXINTCELL_SIZE;
            break;

        case PGTYPE_INDEX_LEAF:
            // 按照叶索引的格式存储, 从free space中得到其存储空间的起始地址
            cell_pointer = data + btn->cells_offset - INDEXLEAFCELL_SIZE;
            // 字节0-3存储常量字节
            memcpy(cell_pointer, const_bytes, 4);
            // 字节4-7存储Key字段
            put4byte(cell_pointer + 4, cell->key);
            // 字节8-11存储KeyPK字段
            put4byte(cell_pointer + 8, cell->fields.indexLeaf.keyPk);
            // 更新btn的单元格偏移量
            btn->cells_offset -= INDEXLEAFCELL_SIZE;
            break;

    default:
        abort();
    }

    // 将ncell后的单元格向后移, 保证有序
    memmove(btn->celloffset_array + (ncell * 2) + 2, btn->celloffset_array + (ncell * 2), (btn->n_cells - ncell) * 2);
    // 将单元格数据的偏移量存储到其对应的偏移数组的位置中
    put2byte(btn->celloffset_array + (ncell * 2), btn->cells_offset);
    // 更新单元格数量增加一
    btn->n_cells++;
    // 更新free space的偏移量增加二
    btn->free_offset += 2;
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
    BTreeCell cell;
    BTreeNode *btn;

    int status;
    // 尝试根据页码读取结点
    if ((status = chidb_Btree_getNodeByPage(bt, nroot, &btn)) != CHIDB_OK)
    {
        // 读取失败返回错误码
        return status;
    }

    int i;
    // 遍历页中所有的Cell
    for (i = 0; i < btn->n_cells; ++i)
    {
        // 根据Cell的索引获取Cell
        chidb_Btree_getCell(btn, i, &cell);
        // 若键匹配且Cell类型为Table Leaf
        if ((cell.key == key) && (btn->type == PGTYPE_TABLE_LEAF))
        {
            // 将结点存储的数据大小存储到size指向的空间
            *size = cell.fields.tableLeaf.data_size;
            // 为传出的data分配内存
            (*data) = malloc(sizeof(uint8_t) * (*size));
            // 若分配内存失败
            if (!(*data))
            {
                // 释放内存结点(btn), 返回错误码
                chidb_Btree_freeMemNode(bt, btn);
                return CHIDB_ENOMEM;
            }
            // 复制Cell中的数据到*data指向的内存空间
            memcpy(*data, cell.fields.tableLeaf.data, *size);
            // 尝试释放btn结点所占的内存
            if ((status = chidb_Btree_freeMemNode(bt, btn)) != CHIDB_OK)
            {
                // 失败则返回错误码
                return status;
            }
            return CHIDB_OK;
        }
        // 若Cell中的键小于或等于给定key, 因为页中cell是有序的, 所以第一个大于或等于给定key的结点一定是包含所找Cell的页码的Cell
        else if (cell.key >= key)
        {
            // 保存btn的类型
            uint8_t temp_type = btn->type;
            // 尝试释放btn, 失败则返回错误码
            if ((status = chidb_Btree_freeMemNode(bt, btn)) != CHIDB_OK)
            {
                return status;
            }
            // 若类型非表叶子结点则在子页中查找
            if (temp_type != PGTYPE_TABLE_LEAF)
            {
                return chidb_Btree_find(bt, cell.fields.tableInternal.child_page, key, data, size);
            }
            // 否则返回未找到
            else
            {
                return CHIDB_ENOTFOUND;
            }
        }
    }

    // 若遍历页中的cells没有找到满足条件, 并且页非叶结点, 则在btn指向的右边页中继续查找
    if (btn->type != PGTYPE_TABLE_LEAF)
    {
        i = btn->right_page;
        // 尝试释放btn, 失败则返回错误码
        if ((status = chidb_Btree_freeMemNode(bt, btn)) != CHIDB_OK)
        {
            return status;
        }
        // 在right page中继续查找
        return chidb_Btree_find(bt, i, key, data, size);
    }

    // 上述操作均未找到返回未找到
    return CHIDB_ENOTFOUND;
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

// 检查btn是否具有空间存储btc
int hasRoomForCell(BTreeNode *btn, BTreeCell *btc)
{
    // space表示当前结点剩余的可用空间大小
    int size = 0,
        space = btn->cells_offset - btn->free_offset;

    // 根据当前结点的类型更新size
    switch (btn->type)
    {
    case PGTYPE_TABLE_LEAF:
        size = TABLELEAFCELL_SIZE_WITHOUTDATA + btc->fields.tableLeaf.data_size;
        break;
    case PGTYPE_TABLE_INTERNAL:
        size = TABLEINTCELL_SIZE;
        break;
    case PGTYPE_INDEX_LEAF:
        size = INDEXLEAFCELL_SIZE;
        break;
    case PGTYPE_INDEX_INTERNAL:
        size = INDEXINTCELL_SIZE;
        break;
    default:
        break;
    }

    // 如果当前结点剩余的可用空间大于单元格的数据大小
    // 返回1, 否则返回0
    if (space >= size)
    {
        return 1;
    }
    else
    {
        return 0;
    }
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
    // 插入Cell到指定页

    // 尝试读取结点, 错误返回错误码
    BTreeNode *root;
    int status = chidb_Btree_getNodeByPage(bt, nroot, &root); CHECK;

    // 如果有空间, 直接调用insertNonFull
    if (hasRoomForCell(root, btc))
    {
        return chidb_Btree_insertNonFull(bt, nroot, btc);
    }

    BTreeNode *new_child;
    npage_t new_child_num;
    // 准备一个新结点, 包含原父结点的内容
    status = chidb_Btree_newNode(bt, &new_child_num, root->type); CHECK;
    // 读取新结点
    status = chidb_Btree_getNodeByPage(bt, new_child_num, &new_child); CHECK;

    // 遍历Cells插入到新结点中
    int i;
    for (i = 0; i < root->n_cells; ++i)
    {
        BTreeCell cell;
        // 读取当前Cell
        status = chidb_Btree_getCell(root, i, &cell); CHECK;
        // 插入当前Cell到新结点
        status = chidb_Btree_insertCell(new_child, i, &cell); CHECK;
    }

    // 如果根节点为内部结点, 保存right page字段
    switch(root->type)
    {
    case PGTYPE_INDEX_INTERNAL:
    case PGTYPE_TABLE_INTERNAL:
        new_child->right_page = root->right_page;
        break;
    default:
        break;
    }

    // 写入文件并释放结点
    status = chidb_Btree_writeNode(bt, new_child); CHECK;
    status = chidb_Btree_freeMemNode(bt, new_child); CHECK;

    // 保存根节点的类型
    uint8_t type = root->type;
    // 在重新初始化结点之前写入文件并释放它
    status = chidb_Btree_writeNode(bt, root); CHECK;
    status = chidb_Btree_freeMemNode(bt, root); CHECK;

    // 根据根节点的类型初始化一个空的根节点
    switch(type)
    {
    case PGTYPE_INDEX_LEAF:
    case PGTYPE_INDEX_INTERNAL:
        status = chidb_Btree_initEmptyNode(bt, nroot, PGTYPE_INDEX_INTERNAL); CHECK;
        break;
    case PGTYPE_TABLE_LEAF:
    case PGTYPE_TABLE_INTERNAL:
        status = chidb_Btree_initEmptyNode(bt, nroot, PGTYPE_TABLE_INTERNAL); CHECK;
        break;
    }

    // 重新打开根节点
    status = chidb_Btree_getNodeByPage(bt, nroot, &root); CHECK;

    // 更新根节点的right page指向新创建的结点
    root->right_page = new_child_num;

    // 写入并释放根节点
    status = chidb_Btree_writeNode(bt, root); CHECK;
    status = chidb_Btree_freeMemNode(bt, root); CHECK;

    // 切分原来的根节点, 并将产生的cell添加到新的根节点中
    npage_t lower_num;
    status = chidb_Btree_split(bt, nroot, new_child_num, 0, &lower_num); CHECK;

    return chidb_Btree_insertNonFull(bt, nroot, btc);
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
    BTreeNode *btn;
    int status = chidb_Btree_getNodeByPage(bt, npage, &btn); CHECK;

    BTreeNode *child_btn;
    npage_t child_num;

    // 遍历每一个cell
    int i;
    for (i = 0; i < btn->n_cells; ++i)
    {
        // 获取当前Cell
        BTreeCell cell;
        status = chidb_Btree_getCell(btn, i, &cell); CHECK;

        // 如果当前Cell的key与要插入的Cell的key相同, 且非页表内部结点, 则返回重定义错误
        if ((cell.key == btc->key)
            && (btn->type != PGTYPE_TABLE_INTERNAL))
        {
            status = chidb_Btree_freeMemNode(bt, btn); CHECK;
            return CHIDB_EDUPLICATE;
        }

        // 如果要插入的结点key小于等于当前Cell的key, 则可插入在当前位置或当前cell指向的child page中
        if (btc->key <= cell.key)
        {
            switch(btn->type)
            {
            case PGTYPE_TABLE_INTERNAL:
                status = chidb_Btree_freeMemNode(bt, btn); CHECK;
                // 获取当前Cell指向的子结点
                status = chidb_Btree_getNodeByPage(bt, cell.fields.tableInternal.child_page, &child_btn);
                CHECK;

                // 如果当前cell指向的子页没有足够的空间
                if (!hasRoomForCell(child_btn, btc))
                {
                    // 释放child_btn
                    status = chidb_Btree_freeMemNode(bt,child_btn); CHECK;
                    // 切分当前cell指向的子结点, 并将产生的cell插入到这里
                    status = chidb_Btree_split(bt, npage, cell.fields.tableInternal.child_page, i, &child_num);
                    CHECK;
                    // 在切分之后调用insert, 因为当前结点可能没有足够的空间
                    return chidb_Btree_insert(bt, npage, btc);
                }
                // 如果有足够的空间, 在当前cell指向的子结点上调用本函数
                return chidb_Btree_insertNonFull(bt, cell.fields.tableInternal.child_page, btc);

            case PGTYPE_INDEX_INTERNAL:
                status = chidb_Btree_freeMemNode(bt, btn); CHECK;
                // 获取当前Cell指向的子结点
                status = chidb_Btree_getNodeByPage(bt, cell.fields.indexInternal.child_page, &child_btn);
                CHECK;

                // 如果当前cell指向的子页没有足够的空间
                if (!hasRoomForCell(child_btn, btc))
                {
                    // 释放child_btn
                    status = chidb_Btree_freeMemNode(bt,child_btn); CHECK;
                    // 切分当前cell指向的子结点, 并将产生的cell插入到这里
                    status = chidb_Btree_split(bt, npage, cell.fields.indexInternal.child_page, i, &child_num);
                    CHECK;
                    // 在切分之后调用insert, 因为当前结点可能没有足够的空间
                    return chidb_Btree_insert(bt, npage, btc);
                }
                // 如果有足够的空间, 在当前cell指向的子结点上调用本函数
                return chidb_Btree_insertNonFull(bt, cell.fields.indexInternal.child_page, btc);

            // 如果是叶子结点, 则可以直接插入
            case PGTYPE_TABLE_LEAF:
            case PGTYPE_INDEX_LEAF:
                status = chidb_Btree_insertCell(btn, i, btc);
                // 写入文件后释放
                chidb_Btree_writeNode(bt, btn);
                chidb_Btree_freeMemNode(bt, btn);
                return status;
            }
        }
    }

    // 若循环没有找到可以插入的地方, 则应插入在最后或者插入在right page指向的页中
    // 若当前结点为叶子结点, 则直接插入在最后
    if ((btn->type == PGTYPE_INDEX_LEAF) || (btn->type == PGTYPE_TABLE_LEAF))
    {
        status = chidb_Btree_insertCell(btn, i, btc);
        chidb_Btree_writeNode(bt, btn);
        chidb_Btree_freeMemNode(bt, btn);
        return status;
    }
    else
    {
        // 保存right page的值
        npage_t right_page = btn->right_page;
        // 释放btn
        chidb_Btree_freeMemNode(bt, btn);

        // 读取right page指向的页结点
        status = chidb_Btree_getNodeByPage(bt, right_page, &child_btn); CHECK;

        // 如果right page没有足够的空间插入结点
        if (!hasRoomForCell(child_btn, btc))
        {
            status = chidb_Btree_freeMemNode(bt,child_btn); CHECK;
            // 切分right page指向的子结点, 并将产生的cell插入到最后
            status = chidb_Btree_split(bt, npage, right_page, i, &child_num); CHECK;
            // 在切分之后调用insert, 因为当前结点可能没有足够的空间
            return chidb_Btree_insert(bt, npage, btc);
        }
        // 如果有足够的空间, 在当前cell指向的子结点上调用本函数
        return chidb_Btree_insertNonFull(bt, right_page, btc);
    }
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
    // 读取要切分的结点的父结点
    BTreeNode *parent;
    int status = chidb_Btree_getNodeByPage(bt, npage_parent, &parent); CHECK;

    // 读取要切分的结点
    BTreeNode *child;
    status = chidb_Btree_getNodeByPage(bt, npage_child, &child); CHECK;

    // 中间索引为n_cells / 2
    int median_index = child->n_cells / 2;

    // 新建一个结点用于存储切分的左半部分cells
    npage_t left_num;
    status = chidb_Btree_newNode(bt, &left_num, child->type); CHECK;

    // 读取新建的结点
    BTreeNode *left;
    status = chidb_Btree_getNodeByPage(bt, left_num, &left); CHECK;

    // 将要切分的结点的中间Cell提升到父结点中
    BTreeCell mcell, to_insert_cell;
    status = chidb_Btree_getCell(child, median_index, &mcell); CHECK;
    to_insert_cell.type = parent->type;
    to_insert_cell.key = mcell.key;
    switch(parent->type)
    {
        case PGTYPE_TABLE_INTERNAL:
            // 若为页表内部结点, cell的子结点指向新建的左边的页
            to_insert_cell.fields.tableInternal.child_page = left_num;
            break;

        case PGTYPE_INDEX_INTERNAL:
            // 若为索引内部结点, cell的子结点指向新建的左边的页
            to_insert_cell.fields.indexInternal.child_page = left_num;
            // 更新要插入cell的keyPk
            if (mcell.type == PGTYPE_INDEX_INTERNAL)
            {
                to_insert_cell.fields.indexInternal.keyPk = mcell.fields.indexInternal.keyPk;
            }
            else
            {
                to_insert_cell.fields.indexInternal.keyPk = mcell.fields.indexLeaf.keyPk;
            }
            break;
    }
    // 插入到父结点中
    status = chidb_Btree_insertCell(parent, parent_ncell, &to_insert_cell); CHECK;

    BTreeCell cell;
    // 将左半边的cells插入到左边的页中
    int i;
    for (i = 0; i < median_index; ++i)
    {
        status = chidb_Btree_getCell(child, i, &cell); CHECK;
        status = chidb_Btree_insertCell(left, i, &cell); CHECK;
    }

    // 如果需要中间cell的话则插入到左页中
    status = chidb_Btree_getCell(child, i, &cell); CHECK;
    // 如果为叶结点, 则直接插入到左页中
    if (child->type == PGTYPE_TABLE_LEAF)
    {
        status = chidb_Btree_insertCell(left, i++, &cell); CHECK;
    }
    // 如果中间cell不是索引叶子结点, 则需要将左结点的right page指向中间cell的前一个child page
    else if (cell.type != PGTYPE_INDEX_LEAF)
    {
        switch(cell.type)
        {
        case PGTYPE_TABLE_INTERNAL:
            left->right_page = cell.fields.tableInternal.child_page;
            break;
        case PGTYPE_INDEX_INTERNAL:
            left->right_page = cell.fields.indexInternal.child_page;
            break;
        }
    }

    BTreeNode *right;
    // 在npage_child的位置新建一个空结点使right指向
    status = chidb_Btree_initEmptyNode(bt, npage_child, child->type); CHECK;
    status = chidb_Btree_getNodeByPage(bt, npage_child, &right); CHECK;

    // 将中间之后的cells插入到right中
    int j;
    for (j = 0; i < child->n_cells; i++, j++)
    {
        status = chidb_Btree_getCell(child, i, &cell); CHECK;
        status = chidb_Btree_insertCell(right, j, &cell); CHECK;
    }
    right->right_page = child->right_page;

    // 释放child
    status = chidb_Btree_freeMemNode(bt, child);

    // 将修改后的页写入文件
    status = chidb_Btree_writeNode(bt, parent); CHECK;
    status = chidb_Btree_writeNode(bt, right); CHECK;
    status = chidb_Btree_writeNode(bt, left); CHECK;

    // 设置传出参数
    *npage_child2 = left_num;

    // 释放结点
    chidb_Btree_freeMemNode(bt, parent);
    chidb_Btree_freeMemNode(bt, right);
    chidb_Btree_freeMemNode(bt, left);

    return CHIDB_OK;
}

// --------- My Code End ---------
