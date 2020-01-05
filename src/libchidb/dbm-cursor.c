/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors
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


#include "dbm-cursor.h"

/* Creates a new cursor trail and allocates memory for it
 *
 * Return
 * - CHIDB_OK: Operation sucessful
 * - CHIDB_ENOMEM: Malloc failed
 */
int chidb_dbm_cursor_trail_new(BTree *bt, chidb_dbm_cursor_trail_t **ct, npage_t npage, uint32_t depth)
{
    (*ct) = malloc(sizeof(chidb_dbm_cursor_trail_t));
    if((*ct)==NULL)
        return CHIDB_ENOMEM;

    BTreeNode *btn;
    //通过页号获得Node地址
    int ret = chidb_Btree_getNodeByPage(bt, npage, &btn);
    if(ret != CHIDB_OK)
        return ret;

    // 初始化，设置list中元素的位置，对应的node地址和子页面cell
    (*ct)->depth = depth;
    (*ct)->btn = btn;
    (*ct)->n_current_cell = 0;

    return CHIDB_OK;
}

//销毁链表，释放每个元素所存的页面
int chidb_dbm_cursor_trail_list_destroy(BTree *bt, list_t *restrict l)
{
    // free all of the btn's held within the cursor trail structs and all of the trail structs
    chidb_dbm_cursor_trail_t *ct;

    while(!list_empty(l))
    {
        ct = (chidb_dbm_cursor_trail_t *)list_fetch(l);
        chidb_Btree_freeMemNode(bt, ct->btn);
        free(ct);
    }

    list_destroy(l);

    return CHIDB_OK;
}

//复制某个游标中存的所有trail 元素到新的trail中，得到一个副本
int chidb_dbm_cursor_trail_cpy(BTree *bt, list_t *restrict l1, list_t *restrict l2)
{
    chidb_dbm_cursor_trail_t *ct_next, *ct_next_cpy;
    BTreeNode *btncpy;

    //初始化副本链表
    list_init(l2);

    list_iterator_start(l1);
    //用迭代器依次获得节点并复制
    while(list_iterator_hasnext(l1))
    {
        ct_next = (chidb_dbm_cursor_trail_t *)list_iterator_next(l1);

        // make a deep copy of the element
        ct_next_cpy = malloc(sizeof(chidb_dbm_cursor_t));
        if(ct_next_cpy == NULL)
            return CHIDB_ENOMEM;

        ct_next_cpy->depth = ct_next->depth;
        chidb_Btree_getNodeByPage(bt, ct_next->btn->page->npage, &btncpy);
        ct_next_cpy->btn = btncpy;
        ct_next_cpy->n_current_cell = ct_next->n_current_cell;

        // append the new element to the new list
        list_append(l2, ct_next_cpy);
    }
    list_iterator_stop(l1);

    return CHIDB_OK;
}
//从某个深度开始清除之后的节点
int chidb_dbm_cursor_clear_trail_from(BTree *bt, chidb_dbm_cursor_t *c, uint32_t depth)
{
    int i;
    chidb_dbm_cursor_trail_t *ct;

    for(i = list_size(&(c->trail))-1; i > depth; i--)
    {
        ct = list_extract_at(&(c->trail), i); // remove element from list

        // free resources associated with the trail element
        chidb_Btree_freeMemNode(bt, ct->btn);
        free(ct);
    }
    return CHIDB_OK;
}
//移除某个深度处的链表节点
int chidb_dbm_cursor_trail_remove_at(BTree *bt, chidb_dbm_cursor_t *c, uint32_t depth)
{
    chidb_dbm_cursor_trail_t *ct = list_extract_at(&(c->trail), depth);
    chidb_Btree_freeMemNode(bt, ct->btn);
    free(ct);

    return CHIDB_OK;
}
//初始化cursor 记录对应的页，初始化该游标中的trail，并将root_page 对应的trail节点放入trail链表中
int chidb_dbm_cursor_init(BTree *bt, chidb_dbm_cursor_t *c, npage_t root_page, ncol_t n_cols)
{
    int rc;
    BTreeNode *btn;
    chidb_dbm_cursor_trail_t *ct;

    // allocate space for the root btn in the trail
    ct = malloc(sizeof(chidb_dbm_cursor_trail_t));
    if(ct == NULL)
        return CHIDB_ENOMEM;

    if((rc = chidb_Btree_getNodeByPage(bt, root_page, &btn)) != CHIDB_OK)
        return rc;

    ct->depth = 0;
    ct->btn = btn;
    ct->n_current_cell = 0;

    list_init(&(c->trail));

    c->root_page = root_page;
    c->root_type = btn->type;
    c->n_cols = n_cols;
    list_insert_at(&(c->trail), ct, ct->depth);

    return CHIDB_OK;
}

//销毁游标
int chidb_dbm_cursor_destroy(BTree *bt, chidb_dbm_cursor_t *c)
{
    chidb_dbm_cursor_trail_list_destroy(bt, &(c->trail));

    return CHIDB_OK;
}

//将游标所指的cell向后移动一个，
/*
    只有游标对应的页为B树中的叶节点才会移动，当trail 中所指示的cell已经是最后一个cell后要把这个trail移除，
    再从上一个节点（实际上是这个leaf的父节点）中尝试移动。
    ----------------------------------------------------------
    更换到上一个trail节点后，自增当前cell，如果自增后没有超出页中cell个数，则调用chidb_dbm_cursorTable_fwdDwn向前移动
    在chidb_dbm_cursorTable_fwdDwn中如果节点为PGTYPE_TABLE_INTERNAL，则根据新的trail中的n_current_cell的大小，将这cell所指的子页面或者RIGHT_PAGE
    加入到trail链表中，再次调用chidb_dbm_cursorTable_fwdDwn
    如果是叶子节点，就自增，结束移动
    -------------------------------------------------------------
    如果所有的trail节点表明自己移动到了最后，则移动失败


    ------------------------------------------------------------
    移动索引树的游标时根据索引树的特点，在判断cell是否超出页中cell个数有相应的调整
*/
int chidb_dbm_cursor_fwd(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;
    int ret = CHIDB_OK; // to quiet compiler warnings

    list_t trail_copy;
    chidb_dbm_cursor_trail_cpy(bt, &(c->trail), &trail_copy);

    switch(node_type)
    {
        case PGTYPE_TABLE_INTERNAL:
        case PGTYPE_TABLE_LEAF:
            ret = chidb_dbm_cursorTable_fwd(bt, c);
            break;

        case PGTYPE_INDEX_INTERNAL:
        case PGTYPE_INDEX_LEAF:
            ret = chidb_dbm_cursorIndex_fwd(bt, c);
            break;

        default:
            ret = CHIDB_ETYPE;
            break;
    }

    if(ret == CHIDB_CURSORCANTMOVE)
    {
        c->trail = trail_copy;
    }
    else
    {
        chidb_dbm_cursor_trail_list_destroy(bt, &trail_copy);
    }

    return ret;
}


int chidb_dbm_cursorTable_fwd(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    if(c->current_cell.type != PGTYPE_TABLE_LEAF)
        return CHIDB_ETYPE;

    if(ct->n_current_cell == ct->btn->n_cells - 1)
    {
        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);

        return chidb_dbm_cursorTable_fwdUp(bt, c);
    }
    else
    {
        ct->n_current_cell++;
        chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

        return CHIDB_OK;
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorTable_fwdUp(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;

    if(list_loc == -1)
    {
        return CHIDB_CURSORCANTMOVE;
    }

    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    if(ct->btn->type != PGTYPE_TABLE_INTERNAL)
    {
        return CHIDB_ETYPE;
    }

    ct->n_current_cell++;

    if(ct->n_current_cell <= ct->btn->n_cells)
    {

        return chidb_dbm_cursorTable_fwdDwn(bt,c);
    }
    else
    {

        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


        return chidb_dbm_cursorTable_fwdUp(bt, c);
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorTable_fwdDwn(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    npage_t pg;
    switch(node_type)
    {
        case PGTYPE_TABLE_INTERNAL:
            if(ct->n_current_cell < ct->btn->n_cells)
            {
                BTreeCell cell;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);
                pg = cell.fields.tableInternal.child_page;
            }
            else if(ct->n_current_cell == ct->btn->n_cells)
            {
                pg = ct->btn->right_page;
            }
            else
                return CHIDB_ECELLNO;

            chidb_dbm_cursor_trail_t *ct_new;
            uint32_t next_depth = list_loc + 1;
            chidb_dbm_cursor_trail_new(bt, &ct_new, pg, next_depth);

            list_insert_at(&(c->trail), ct_new, next_depth);

            return chidb_dbm_cursorTable_fwdDwn(bt, c);

        case PGTYPE_TABLE_LEAF:

            chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

            return CHIDB_OK;
        default:
            return CHIDB_ETYPE;
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorIndex_fwd(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    switch(node_type)
    {
        case PGTYPE_INDEX_LEAF:
            if(ct->n_current_cell == ct->btn->n_cells - 1)
            {

                chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


                return chidb_dbm_cursorIndex_fwdUp(bt, c);
            }
            else
            {
                ct->n_current_cell++;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

                return CHIDB_OK;
            }
        case PGTYPE_INDEX_INTERNAL:

            ct->n_current_cell++;
            if(ct->n_current_cell <= ct->btn->n_cells)
            {

                return chidb_dbm_cursorIndex_fwdDwn(bt,c);
            }
            else
            {

                chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


                return chidb_dbm_cursorIndex_fwdUp(bt, c);
            }
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorIndex_fwdUp(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;

    if(list_loc == -1)
    {
        return CHIDB_CURSORCANTMOVE;
    }

    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    ct->n_current_cell++;

    if(ct->n_current_cell < ct->btn->n_cells)
    {

        BTreeCell cell;
        chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);

        return CHIDB_OK;
    }
    else if(ct->n_current_cell == ct->btn->n_cells)
    {

        return chidb_dbm_cursorIndex_fwdDwn(bt,c);
    }
    else
    {

        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


        return chidb_dbm_cursorIndex_fwdUp(bt, c);
    }

    return CHIDB_OK;
}


int chidb_dbm_cursorIndex_fwdDwn(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    npage_t pg;
    switch(node_type)
    {
        case PGTYPE_INDEX_INTERNAL:
            if(ct->n_current_cell < ct->btn->n_cells)
            {
                BTreeCell cell;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);
                pg = cell.fields.indexInternal.child_page;
            }
            else if(ct->n_current_cell == ct->btn->n_cells)
            {
                pg = ct->btn->right_page;
            }
            else
                return CHIDB_ECELLNO;

            chidb_dbm_cursor_trail_t *ct_new;
            uint32_t next_depth = list_loc + 1;
            chidb_dbm_cursor_trail_new(bt, &ct_new, pg, next_depth);

            list_insert_at(&(c->trail), ct_new, next_depth);

            return chidb_dbm_cursorIndex_fwdDwn(bt, c);

        case PGTYPE_INDEX_LEAF:
            chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

            return CHIDB_OK;
        default:
            return CHIDB_ETYPE;
    }

    return CHIDB_OK;
}

//将游标所指的cell向前移动一个，
/*
    只有游标对应的页为树中的叶节点才会移动，当trail 中所指示的cell已经是第一个cell后要把这个trail移除，
    再从上一个节点（实际上是这个leaf的父节点）中尝试移动。
    ----------------------------------------------------------
    更换到上一个trail节点后，自减当前cell，如果自减后仍然存在节点，则调用chidb_dbm_cursorTable_revDwn向前移动
    在chidb_dbm_cursorTable_revDwn中如果节点为PGTYPE_TABLE_INTERNAL，则根据新的trail中的n_current_cell的大小，将这cell所指的子页面或者RIGHT_PAGE
    加入到trail链表中，再次调用chidb_dbm_cursorTable_revDwn
    如果是叶子节点，就自减，结束移动
    -------------------------------------------------------------
    如果所有的trail节点表明自己已经是第一个cell，则移动失败


    ------------------------------------------------------------
    移动索引树的游标时根据索引树的特点，在判断cell是否超出页中cell个数有相应的调整
*/
int chidb_dbm_cursor_rev(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    uint8_t node_type = ct->btn->type;
    int ret = CHIDB_OK;
    switch(node_type)
    {
        case PGTYPE_TABLE_INTERNAL:
        case PGTYPE_TABLE_LEAF:
            ret = chidb_dbm_cursorTable_rev(bt, c);
            break;

        case PGTYPE_INDEX_INTERNAL:
        case PGTYPE_INDEX_LEAF:
            ret = chidb_dbm_cursorIndex_rev(bt, c);
            break;

        default:
            ret = CHIDB_ETYPE;
            break;
    }
    return ret;
}

int chidb_dbm_cursorTable_rev(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    if(c->current_cell.type != PGTYPE_TABLE_LEAF)
        return CHIDB_ETYPE;

    if(ct->n_current_cell == 0)
    {

        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);

        return chidb_dbm_cursorTable_revUp(bt, c);
    }
    else
    {
        ct->n_current_cell--;
        chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

        return CHIDB_OK;
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorTable_revUp(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;

    if(list_loc == -1)
    {
        return CHIDB_CURSORCANTMOVE;
    }

    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    if(ct->btn->type != PGTYPE_TABLE_INTERNAL)
    {
        return CHIDB_ETYPE;
    }

    ct->n_current_cell--;

    if(ct->n_current_cell >= 0)
    {
        return chidb_dbm_cursorTable_revDwn(bt,c);
    }
    else
    {
        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);

        return chidb_dbm_cursorTable_revUp(bt,c);
    }

    return CHIDB_OK;
}

int chidb_dbm_cursorTable_revDwn(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    npage_t pg;
    switch(node_type)
    {
        case PGTYPE_TABLE_INTERNAL:
            if(ct->n_current_cell < ct->btn->n_cells)
            {
                BTreeCell cell;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);
                pg = cell.fields.tableInternal.child_page;
            }
            else if(ct->n_current_cell == ct->btn->n_cells)
            {
                pg = ct->btn->right_page;
            }
            else
                return CHIDB_ECELLNO;

            chidb_dbm_cursor_trail_t *ct_new;
            uint32_t next_depth = list_loc + 1;
            chidb_dbm_cursor_trail_new(bt, &ct_new, pg, next_depth);

            ct_new->n_current_cell = ct_new->btn->n_cells;


            if(ct_new->btn->type == PGTYPE_TABLE_LEAF)
                ct_new->n_current_cell--;


            list_insert_at(&(c->trail), ct_new, next_depth);


            return chidb_dbm_cursorTable_revDwn(bt, c);

        case PGTYPE_TABLE_LEAF:
            chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

            return CHIDB_OK;

        default:
            return CHIDB_ETYPE;
    }

    return CHIDB_OK;
}


int chidb_dbm_cursorIndex_rev(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    switch(node_type)
    {
        case PGTYPE_INDEX_LEAF:

            if(ct->n_current_cell == 0)
            {

                chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


                return chidb_dbm_cursorIndex_revUp(bt, c);
            }
            else
            {
                ct->n_current_cell--;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

                return CHIDB_OK;
            }
        case PGTYPE_INDEX_INTERNAL:

            if(ct->n_current_cell >= 0)
            {

                return chidb_dbm_cursorIndex_revDwn(bt,c);
            }
            else
            {

                chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


                return chidb_dbm_cursorIndex_revUp(bt, c);
            }
    }

    return CHIDB_OK;
}


int chidb_dbm_cursorIndex_revUp(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;

    if(list_loc == -1)
    {
        return CHIDB_CURSORCANTMOVE;
    }

    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);

    ct->n_current_cell--;

    if(ct->n_current_cell >= 0)
    {

        BTreeCell cell;
        chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);

        return CHIDB_OK;
    }
    else
    {

        chidb_dbm_cursor_trail_remove_at(bt, c, list_loc);


        return chidb_dbm_cursorIndex_fwdUp(bt, c);
    }

    return CHIDB_OK;
}


int chidb_dbm_cursorIndex_revDwn(BTree *bt, chidb_dbm_cursor_t *c)
{
    uint32_t list_loc = list_size(&(c->trail)) - 1;
    chidb_dbm_cursor_trail_t *ct = list_get_at(&(c->trail), list_loc);
    uint8_t node_type = ct->btn->type;

    npage_t pg;
    switch(node_type)
    {
        case PGTYPE_INDEX_INTERNAL:
            if(ct->n_current_cell >= 0)
            {

                BTreeCell cell;
                chidb_Btree_getCell(ct->btn, ct->n_current_cell, &cell);
                pg = cell.fields.indexInternal.child_page;
            }
            else if(ct->n_current_cell == ct->btn->n_cells)
            {

                pg = ct->btn->right_page;
            }
            else
                return CHIDB_ECELLNO;


            chidb_dbm_cursor_trail_t *ct_new;
            uint32_t next_depth = list_loc + 1;
            chidb_dbm_cursor_trail_new(bt, &ct_new, pg, next_depth);


            ct_new->n_current_cell = ct_new->btn->n_cells;


            if(ct_new->btn->type == PGTYPE_TABLE_LEAF)
                ct_new->n_current_cell--;

            list_insert_at(&(c->trail), ct_new, next_depth);

            return chidb_dbm_cursorIndex_fwdDwn(bt, c);

        case PGTYPE_INDEX_LEAF:

            chidb_Btree_getCell(ct->btn, ct->n_current_cell, &(c->current_cell));

            return CHIDB_OK;

        default:
            return CHIDB_ETYPE;
    }

    return CHIDB_OK;
}

/*



*/
int chidb_dbm_cursor_seek(BTree *bt, chidb_dbm_cursor_t *c, chidb_key_t key, npage_t next, int depth, int seek_type)
{
    chidb_dbm_cursor_trail_t *trail_entry;

    if (!depth)
    {
        chidb_dbm_cursor_clear_trail_from(bt, c, 0);
        next = c->root_page;
        trail_entry = list_get_at(&(c->trail), 0);
    }
    else
    {
        trail_entry = malloc(sizeof(chidb_dbm_cursor_trail_t));
    }

    BTreeCell cell;
    BTreeNode *btn;


    int status;
    int i = 0;

    if ((status = chidb_Btree_getNodeByPage(bt, next, &btn)) != CHIDB_OK)
    {
        free(btn);
        return status;
    }

    trail_entry->depth = depth;
    trail_entry->btn = btn;

    if (btn->type == PGTYPE_TABLE_LEAF)
    {
        do
        {
            if (chidb_Btree_getCell(btn, i, &cell) != CHIDB_OK)
                return CHIDB_ECELLNO;

            if (cell.key == key)
            {
                trail_entry->n_current_cell = i;
                c->current_cell = cell;
                if (depth)
                    list_append(&c->trail, trail_entry);

                if (seek_type == SEEKLT)
                    return chidb_dbm_cursor_rev(bt, c);

                else if (seek_type == SEEKGT)
                    return chidb_dbm_cursor_fwd(bt, c);

                return CHIDB_OK;
            }
            else if (cell.key > key)
            {
                trail_entry->n_current_cell = i;
                c->current_cell = cell;
                if (depth)
                    list_append(&c->trail, trail_entry);

                if (seek_type == SEEK)
                    return CHIDB_ENOTFOUND;

                else if (seek_type == SEEKLT || seek_type == SEEKLE)
                    return chidb_dbm_cursor_rev(bt, c);

                return CHIDB_OK;
            }
            else if (cell.key < key)
            {
                i++;
            }

        } while (i < btn->n_cells);

        return CHIDB_CURSORCANTMOVE;
    }

    else if (btn->type == PGTYPE_TABLE_INTERNAL)
    {
        do
        {
            if (chidb_Btree_getCell(btn, i, &cell) != CHIDB_OK)
                return CHIDB_ECELLNO;

            if ((cell.key >= key))
            {
                trail_entry->n_current_cell = i;
                c->current_cell = cell;
                if (depth)
                    list_append(&c->trail, trail_entry);

                if ((status = chidb_dbm_cursor_seek(bt, c, key,cell.fields.tableInternal.child_page, depth+1, seek_type)) != CHIDB_OK)
                    return status;
                return CHIDB_OK;
            }
            else
            {
                i++;
            }

        } while ((cell.key < key) && (i < btn->n_cells));


        trail_entry->n_current_cell = btn->n_cells;
        c->current_cell = cell;
        if (depth)
            list_append(&c->trail, trail_entry);

        return chidb_dbm_cursor_seek(bt, c, key, btn->right_page, depth+1, seek_type);
    }

    else
    {
        do
        {
            if (chidb_Btree_getCell(btn, i, &cell) != CHIDB_OK)
                return CHIDB_ECELLNO;

            if (cell.key == key)
            {
                trail_entry->n_current_cell = i;
                c->current_cell = cell;
                if (depth)
                    list_append(&c->trail, trail_entry);

                if (seek_type == SEEKLT)
                    return chidb_dbm_cursor_rev(bt, c);
                else if (seek_type == SEEKGT)
                    return chidb_dbm_cursor_fwd(bt, c);

                return CHIDB_OK;
            }
            else if (cell.key > key)
            {
                trail_entry->n_current_cell = i;
                c->current_cell = cell;
                if (depth)
                    list_append(&c->trail, trail_entry);

                if (btn->type == PGTYPE_INDEX_INTERNAL)
                    return chidb_dbm_cursor_seek(bt, c, key,cell.fields.indexInternal.child_page, depth+1, seek_type);
                else
                {
                    if (seek_type == SEEK)
                        return CHIDB_ENOTFOUND;

                    else if (seek_type == SEEKLT || seek_type == SEEKLE)
                        return chidb_dbm_cursor_rev(bt, c);

                    return CHIDB_OK;
                }
            }

            i++;
        } while (i < btn->n_cells);

        if (btn->type == PGTYPE_INDEX_INTERNAL)
        {
            trail_entry->n_current_cell = btn->n_cells;
            c->current_cell = cell;
            if (depth)
                list_append(&c->trail, trail_entry);

            return chidb_dbm_cursor_seek(bt, c, key, btn->right_page, depth+1, seek_type);
        }

        else if (btn->type == PGTYPE_INDEX_LEAF)
            return CHIDB_CURSORCANTMOVE;
    }

    return CHIDB_OK;
}
