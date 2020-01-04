/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine operations.
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


#include "dbm.h"
#include "btree.h"
#include "record.h"

//一些封装好的操作函数，用于写寄存器
int chidb_dbm_op_WriteReg (chidb_stmt *stmt, int regNo, int reg_type, void *data);
//防止编译器报warring
int realloc_cur(chidb_stmt *stmt, uint32_t size);
int realloc_reg(chidb_stmt *stmt, uint32_t size);

/* Function pointer for dispatch table */
typedef int (*handler_function)(chidb_stmt *stmt, chidb_dbm_op_t *op);

/* Single entry in the instruction dispatch table */
struct handler_entry
{
    opcode_t opcode;
    handler_function func;
};

/* This generates all the instruction handler prototypes. It expands to:
 *
 * int chidb_dbm_op_OpenRead(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * int chidb_dbm_op_OpenWrite(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * ...
 * int chidb_dbm_op_Halt(chidb_stmt *stmt, chidb_dbm_op_t *op);
 */
#define HANDLER_PROTOTYPE(OP) int chidb_dbm_op_## OP (chidb_stmt *stmt, chidb_dbm_op_t *op);
FOREACH_OP(HANDLER_PROTOTYPE)


/* Ladies and gentlemen, the dispatch table. */
#define HANDLER_ENTRY(OP) { Op_ ## OP, chidb_dbm_op_## OP},

struct handler_entry dbm_handlers[] =
{
    FOREACH_OP(HANDLER_ENTRY)
};
/*
 一个用来比较寄存器内容的函数
 返回值定义在 dbm-types.h中
*RETURN :
* EQ 相等
* GT r1>r2
* LT r1<r2
* NE r1!=r2  不相等(用于BIN类型)
* NOTCMP 类型不同或 REG_UNSPECIFIED  REG_NULL  REG_BINARY 的返回
*/

int cmp_reg_content(chidb_dbm_register_t * R1,chidb_dbm_register_t *R2){
    if(R1->type!=R2->type)
        return NOTCMP;
    switch (R1->type)
    {
    case REG_INT32:
        {
            uint32_t intcmp = R1->value.i-R2->value.i;
            if(intcmp==0)
                return EQ;
            else if(intcmp>0)
                return GT;
            else return LT;
            break;
        }
    case REG_STRING:
        {
            int cmp = strcmp(R1->value.s,R2->value.s);
            if(cmp==0)
            return EQ;
            else if(cmp>0)
            return GT;
            else return LT;
            break;
        }
    case REG_BINARY :
    #ifdef CMPBIN
        if(R1->value.bin.nbytes==R2->value.bin.nbytes)
        {
            uint32_t len = R1->value.bin.nbytes;
            for(uint32_t i = 0;i<len;i++)
                if(R1->value.bin.bytes[i]!=R2->value.bin.bytes[i])
                    return NE;
            return EQ;
        }
        else{
            uint32_t len = R1->value.bin.nbytes > R2->value.bin.nbytes ? \
            R2->value.bin.nbytes:R1->value.bin.nbytes;
            uint32_t i;
            for(i = 0;i<len;i++)
                if(R1->value.bin.bytes[i]!=R2->value.bin.bytes[i])
                    return NE;
            if(len==R1->value.bin.nbytes)
                return GT;
            else return LT;

        }
    #else
        return NOTCMP;
    #endif
    default:
        return NOTCMP;
    }
}
int chidb_dbm_op_handle (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return dbm_handlers[op->opcode].func(stmt, op);
}


/*** INSTRUCTION HANDLER IMPLEMENTATIONS ***/


int chidb_dbm_op_Noop (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    //什么也不做
    return CHIDB_OK;
}


int chidb_dbm_op_OpenRead (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // If cursor doesn't exist, allocate it
    if (!EXISTS_CURSOR(stmt, op->p1))
        realloc_cur(stmt, op->p1);

    chidb_dbm_cursor_t *c = &((stmt)->cursors[op->p1]);
    chidb_dbm_cursor_init(stmt->db->bt, c, stmt->reg[op->p2].value.i, op->p3);

    c->type = CURSOR_READ;

    // check that the op worked properly
    if (!IS_VALID_CURSOR(stmt, op->p1))
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_OpenWrite (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // If cursor doesn't exist, allocate it
    if (!EXISTS_CURSOR(stmt, op->p1))
        realloc_cur(stmt, op->p1);

    chidb_dbm_cursor_t *c = &((stmt)->cursors[op->p1]);
    chidb_dbm_cursor_init(stmt->db->bt, c, stmt->reg[op->p2].value.i, op->p3);

    c->type = CURSOR_WRITE;

    // check that the op worked properly
    if (!IS_VALID_CURSOR(stmt, op->p1))
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_Close (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // makes sure we are trying to destroy a valid cursor
    if (!EXISTS_CURSOR(stmt, op->p1))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[op->p1]);
    chidb_dbm_cursor_destroy(stmt->db->bt, c);

    return CHIDB_OK;
}

//只保留cursor中trail链表中第一个节点，然后将cursor中的cell指向第一个叶节点的第一个cell
int chidb_dbm_op_Rewind (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t jmp_addr = op->p2;

    if (!EXISTS_CURSOR(stmt, op->p1))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[op->p1]);

    chidb_dbm_cursor_trail_t *ct = (chidb_dbm_cursor_trail_t*) list_get_at(&c->trail, 0);

    if (ct->btn->n_cells == 0) 
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_PROBLEM;

        stmt->pc = jmp_addr;
    }
    else 
    {
        
        chidb_dbm_cursor_clear_trail_from(stmt->db->bt, c, 0);

        
        ct->n_current_cell = 0;

        
        switch(ct->btn->type)
        {
            case PGTYPE_TABLE_INTERNAL:
            case PGTYPE_TABLE_LEAF:
                chidb_dbm_cursorTable_fwdDwn(stmt->db->bt, c);
                break;
            case PGTYPE_INDEX_INTERNAL:
            case PGTYPE_INDEX_LEAF:
                chidb_dbm_cursorIndex_fwdDwn(stmt->db->bt, c);
                break;
            default:
                return CHIDB_ETYPE;
        }
    }

    return CHIDB_OK;
}

int chidb_dbm_op_Next (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;
    int fwd_ret;

    if (!EXISTS_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    fwd_ret = chidb_dbm_cursor_fwd(stmt->db->bt,c);
    if(fwd_ret != CHIDB_CURSORCANTMOVE)
    {
        if (!IS_VALID_ADDRESS(stmt, op->p2))
            return CHIDB_DONE;
        stmt->pc = jmp_addr;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_Prev (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t jmp_addr = op->p2;
    int fwd_ret;

    if (!EXISTS_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    fwd_ret = chidb_dbm_cursor_rev(stmt->db->bt,c);
    if(fwd_ret != CHIDB_CURSORCANTMOVE)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_DONE;
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}
//不同的seek操作函数
//直接指向所提供的页的某个key对应的cell
int chidb_dbm_op_Seek (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
    {
        fprintf(stderr, "%s\n", "an error has occured in seek");
        return CHIDB_PROBLEM;
    }

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    uint32_t key = r1->value.i;

    int seek_ret;

    if (!IS_VALID_CURSOR(stmt, c_index))
    {
        fprintf(stderr, "%s\n", "a cursor error has occured in seek");
        return CHIDB_PROBLEM;
    }

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    seek_ret = chidb_dbm_cursor_seek(stmt->db->bt, c, key, c->root_page, 0, SEEK);

    if(seek_ret != CHIDB_OK)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
        {
            fprintf(stderr, "%s\n", "error, invalid jump address");
            return CHIDB_PROBLEM;
        }
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}
//指向所提供的页的某个key对应的cell的后一个
int chidb_dbm_op_SeekGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    uint32_t key = r1->value.i;

    int seek_ret;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    seek_ret = chidb_dbm_cursor_seek(stmt->db->bt, c, key, c->root_page, 0, SEEKGT);
    if(seek_ret != CHIDB_OK)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_PROBLEM;
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_SeekGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    uint32_t key = r1->value.i;

    int seek_ret;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    seek_ret = chidb_dbm_cursor_seek(stmt->db->bt, c, key, c->root_page, 0, SEEKGE);
    if(seek_ret != CHIDB_OK)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_PROBLEM;
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_SeekLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    uint32_t key = r1->value.i;

    int seek_ret;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    seek_ret = chidb_dbm_cursor_seek(stmt->db->bt, c, key, c->root_page, 0, SEEKLT);
    if(seek_ret != CHIDB_OK)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_PROBLEM;
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_SeekLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    uint32_t key = r1->value.i;

    int seek_ret;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    seek_ret = chidb_dbm_cursor_seek(stmt->db->bt, c, key, c->root_page, 0, SEEKLE);
    if(seek_ret != CHIDB_OK)
    {
        if (!IS_VALID_ADDRESS(stmt, jmp_addr))
            return CHIDB_PROBLEM;
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}
//按类型将cursor中对应cell的内容存入寄存器中(p3)
int chidb_dbm_op_Column (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t col_num = op->p2;
    int32_t reg_index = op->p3;

    int8_t byte;
    int16_t smallint;
    int32_t integer;
    char *string;

    int ret;
    int type;
    DBRecord *dbr;

    // get cursor and entry data
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);
    uint8_t *entry = c->current_cell.fields.tableLeaf.data;

    if((ret = chidb_DBRecord_unpack(&dbr, entry)) != CHIDB_OK)
        fprintf(stderr,"CHIDB_DBM_OP_COLUMN: couldn't unpack record\n");

    type = chidb_DBRecord_getType(dbr, (uint8_t)col_num);

    switch(type)
    {
        case SQL_INTEGER_1BYTE:
            ret = chidb_DBRecord_getInt8(dbr, (uint8_t)col_num, &byte);
            if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_INT32, &byte) != CHIDB_OK)
                return CHIDB_PROBLEM;
            break;
        case SQL_INTEGER_2BYTE:
            ret = chidb_DBRecord_getInt16(dbr, (uint8_t)col_num, &smallint);
            if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_INT32, &smallint) != CHIDB_OK)
                return CHIDB_PROBLEM;
            break;
        case SQL_INTEGER_4BYTE:
            ret = chidb_DBRecord_getInt32(dbr,(uint8_t)col_num, &integer);
            if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_INT32, &integer) != CHIDB_OK)
                return CHIDB_PROBLEM;
            break;
        case SQL_NULL:
            if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_NULL, NULL) != CHIDB_OK)
                return CHIDB_PROBLEM;
            break;
        case SQL_TEXT:
            ret = chidb_DBRecord_getString(dbr, (uint8_t)col_num, &string);
            if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_STRING, strdup(string)) != CHIDB_OK)
                return CHIDB_PROBLEM;
            break;
        case SQL_NOTVALID:
            (stmt)->reg[reg_index].type = REG_UNSPECIFIED;
            break;
    }

    return CHIDB_OK;
}
//将某个cursor中cell对应的key值存到寄存器中
int chidb_dbm_op_Key (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t reg_index = op->p2;
    uint32_t key;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    key = c->current_cell.key;

    if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_INT32, &key) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_Integer (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    if (chidb_dbm_op_WriteReg(stmt, op->p2, REG_INT32, &(op->p1)) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_String (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    if (chidb_dbm_op_WriteReg(stmt, op->p2, REG_STRING, strdup(op->p4)) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_Null (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    if (chidb_dbm_op_WriteReg(stmt, op->p2, REG_NULL, NULL) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}

int chidb_dbm_op_ResultRow (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    if (!IS_VALID_REGISTER(stmt, op->p1))
        return CHIDB_PROBLEM;
    if (!IS_VALID_REGISTER(stmt, op->p2))
        return CHIDB_PROBLEM;

    stmt->startRR = (uint32_t)op->p1;
    stmt->nRR = (uint32_t)op->p2;

    return CHIDB_ROW;
}


int chidb_dbm_op_MakeRecord (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t r1 = op->p1;
    int32_t n = op->p2;
    int32_t r2 = op->p3;
    int32_t end_reg = (r1+n-1);
    DBRecord *dbr;
    uint8_t *record;

  
    chidb_dbm_register_t *reg2 = &((stmt)->reg[r2]);

    DBRecordBuffer dbrb;
    chidb_DBRecord_create_empty(&dbrb, (uint8_t)n);

    int i;
    for(i = r1; i <= end_reg; i++)
    {
        if (!IS_VALID_REGISTER(stmt, i))
            return CHIDB_PROBLEM;

        chidb_dbm_register_t *tmp = &((stmt)->reg[i]);

        if(tmp->type == REG_NULL)
            chidb_DBRecord_appendNull(&dbrb);
        else if(tmp->type == REG_INT32)
            chidb_DBRecord_appendInt32(&dbrb, tmp->value.i);
        else if(tmp->type == REG_STRING)
            chidb_DBRecord_appendString(&dbrb, tmp->value.s);
    }

    chidb_DBRecord_finalize(&dbrb, &dbr);
    uint32_t packed_len = dbr->packed_len;
    chidb_DBRecord_pack(dbr, &record);
    chidb_DBRecord_destroy(dbr);

   
    if (chidb_dbm_op_WriteReg(stmt, r2, REG_BINARY, NULL) != CHIDB_OK)
        return CHIDB_PROBLEM;

    reg2->type = REG_BINARY;
    reg2->value.bin.nbytes = packed_len;
    reg2->value.bin.bytes = record;

    return CHIDB_OK;
}
//将寄存器中的叶类型cell中的值插入到cursor所指向的B树中
int chidb_dbm_op_Insert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t r1 = op->p2;
    int32_t r2 = op->p3;

    if (!IS_VALID_REGISTER(stmt, r1))
        return CHIDB_PROBLEM;
    if (!IS_VALID_REGISTER(stmt, r2))
        return CHIDB_PROBLEM;
    chidb_dbm_register_t *reg1 = &((stmt)->reg[r1]);
    chidb_dbm_register_t *reg2 = &((stmt)->reg[r2]);

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    BTreeCell *cell = malloc(sizeof(BTreeCell));
    cell->type = PGTYPE_TABLE_LEAF;
    cell->key = (uint32_t)reg2->value.i;

    cell->fields.tableLeaf.data = reg1->value.bin.bytes; 
    cell->fields.tableLeaf.data_size = reg1->value.bin.nbytes;
    chidb_Btree_insert(stmt->db->bt, c->root_page, cell);

    chidb_key_t old_key = c->current_cell.key;
    chidb_dbm_cursor_seek(stmt->db->bt, c, old_key, c->root_page, 0, SEEK);

    free(cell);

    return CHIDB_OK;
}


int chidb_dbm_op_Eq (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp==EQ)
        stmt->pc = op->p2;
    return CHIDB_OK;
}


int chidb_dbm_op_Ne (chidb_stmt *stmt, chidb_dbm_op_t *op)
{

    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp!=EQ)
        stmt->pc = op->p2;
    return CHIDB_OK;
}


int chidb_dbm_op_Lt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{

    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp==LT)
        stmt->pc = op->p2;
    return CHIDB_OK;
}


int chidb_dbm_op_Le (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp==EQ||cmp==LT)
        stmt->pc = op->p2;
    return CHIDB_OK;
}


int chidb_dbm_op_Gt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp==GT)
        stmt->pc = op->p2;

    return CHIDB_OK;
}


int chidb_dbm_op_Ge (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t reg1 = op->p1;
    uint32_t reg2 = op->p3;
    if(reg1>=stmt->nReg||reg2>=stmt->nReg)
        return CHIDB_OK;
    chidb_dbm_register_t R1 = stmt->reg[reg1];
    chidb_dbm_register_t R2 = stmt->reg[reg2];
    int cmp = cmp_reg_content(&R1,&R2);
    if(cmp==EQ||cmp==GT)
        stmt->pc = op->p2;
    return CHIDB_OK;
}


/* IdxGt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) > k, jump
 */
int chidb_dbm_op_IdxGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    if (!IS_VALID_ADDRESS(stmt, jmp_addr))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    int32_t key = r1->value.i;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    // I'm assuming hopefully that current cell points to an index cell
    if(c->current_cell.key > key) {
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_IdxGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t c_index = op->p1;
    uint32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    if (!IS_VALID_ADDRESS(stmt, jmp_addr))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    int32_t key = r1->value.i;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    if((c->current_cell.key > key) || (c->current_cell.key == key)) {
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_IdxLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    if (!IS_VALID_ADDRESS(stmt, jmp_addr))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    int32_t key = r1->value.i;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    
    if(c->current_cell.key < key) {
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_IdxLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t jmp_addr = op->p2;

    if (!IS_VALID_REGISTER(stmt, op->p3))
        return CHIDB_PROBLEM;
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;
    if (!IS_VALID_ADDRESS(stmt, jmp_addr))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *r1 = &((stmt)->reg[op->p3]);
    int32_t key = r1->value.i;

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    if((c->current_cell.key < key) || (c->current_cell.key == key)) {
        stmt->pc = (uint32_t)jmp_addr;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_IdxPKey (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t reg_index = op->p2;
    uint32_t key;

    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    
    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    if(c->current_cell.type == PGTYPE_INDEX_INTERNAL) {
        key = c->current_cell.fields.indexInternal.keyPk;
    }
    else {
        key = c->current_cell.fields.indexLeaf.keyPk;
    }

    if (chidb_dbm_op_WriteReg(stmt, reg_index, REG_INT32, &key) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}


int chidb_dbm_op_IdxInsert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c_index = op->p1;
    int32_t r1 = op->p2;
    int32_t r2 = op->p3;

    if (!IS_VALID_REGISTER(stmt, r1))
        return CHIDB_PROBLEM;
    if (!IS_VALID_REGISTER(stmt, r2))
        return CHIDB_PROBLEM;
    if (!IS_VALID_CURSOR(stmt, c_index))
        return CHIDB_PROBLEM;

    chidb_dbm_register_t *reg1 = &((stmt)->reg[r1]);
    chidb_dbm_register_t *reg2 = &((stmt)->reg[r2]);

    chidb_dbm_cursor_t *c = &((stmt)->cursors[c_index]);

    BTreeCell *cell = malloc(sizeof(BTreeCell));
    cell->type = PGTYPE_INDEX_LEAF;
    cell->key = (uint32_t)reg1->value.i; //grab the idx key

    cell->fields.indexLeaf.keyPk = (uint32_t)reg2->value.i;
    chidb_Btree_insert(stmt->db->bt, c->root_page, cell);

    //RELOADING THE TREE just in case the insert messed up the tree
    chidb_key_t old_key = c->current_cell.key;
    chidb_dbm_cursor_seek(stmt->db->bt, c, old_key, c->root_page, 0, SEEK);

    return CHIDB_OK;
}

//创建一个表，申请一个页面，并且将页号写入寄存器中
int chidb_dbm_op_CreateTable (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    npage_t *root = malloc(sizeof(npage_t));

    int ret = chidb_Btree_newNode(stmt->db->bt, root, PGTYPE_TABLE_LEAF);
    if (ret != CHIDB_OK)
        return ret;

    if (chidb_dbm_op_WriteReg(stmt, op->p1, REG_INT32, root) != CHIDB_OK)
        return CHIDB_PROBLEM;

    free(root);

    return CHIDB_OK;
}


int chidb_dbm_op_CreateIndex (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    npage_t *root = malloc(sizeof(npage_t));

    int ret = chidb_Btree_newNode(stmt->db->bt, root, PGTYPE_INDEX_LEAF);
    if (ret != CHIDB_OK)
        return ret;

    if (chidb_dbm_op_WriteReg(stmt, op->p1, REG_INT32, root) != CHIDB_OK)
        return CHIDB_PROBLEM;

    return CHIDB_OK;
}


int chidb_dbm_op_Copy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}


int chidb_dbm_op_SCopy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}

int chidb_dbm_op_Halt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    exit(op->p1);
    return CHIDB_DONE;
}

int chidb_dbm_op_WriteReg (chidb_stmt *stmt, int regNo, int reg_type, void *data)
{
    if (IS_VALID_REGISTER(stmt, regNo) && regNo > stmt->nReg)
        realloc_reg(stmt, regNo);

    chidb_dbm_register_t *reg = &(stmt->reg[regNo]);
    reg->type = reg_type;

    if (reg_type == REG_INT32)
        reg->value.i = *((int32_t *) data);
    else if (reg_type == REG_STRING)
        reg->value.s = (char *) data;

    return CHIDB_OK;
}
