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

int chidb_dbm_op_handle (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return dbm_handlers[op->opcode].func(stmt, op);
}


/*** INSTRUCTION HANDLER IMPLEMENTATIONS ***/

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
    #ifdef CMPBIN //当需要比较Bit串类型大小的时候
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
int chidb_dbm_op_Noop (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}


int chidb_dbm_op_OpenRead (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_OpenWrite (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Close (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Rewind (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Next (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Prev (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Seek (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_SeekLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_Column (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Key (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Integer (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t the_reg = op->p2;
    uint32_t num = op->p1;
    if(!EXISTS_REGISTER(stmt,the_reg)){
        chidb_dbm_register_t *temp = (chidb_dbm_register_t*)realloc(stmt->reg,sizeof(chidb_dbm_register_t)*(the_reg+1));
        stmt->reg = temp;
        stmt->nReg = the_reg;
    }
    stmt->reg[the_reg].type = REG_INT32;
    stmt->reg[the_reg].value.i = num;
    return CHIDB_OK;

    return CHIDB_OK;
}


int chidb_dbm_op_String (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    uint32_t the_reg = op->p2;
    uint32_t length = op->p1;
    if(!EXISTS_REGISTER(stmt,the_reg)){
        chidb_dbm_register_t *temp = (chidb_dbm_register_t*)realloc(stmt->reg,sizeof(chidb_dbm_register_t)*(the_reg+1));
        stmt->reg = temp;
        stmt->nReg = the_reg;
    }
    free(stmt->reg[the_reg].value.s);
    stmt->reg[the_reg].type = REG_STRING;
    stmt->reg[the_reg].value.s = (char*)malloc(length+1);
    strncpy(stmt->reg[the_reg].value.s,op->p4,length);
    return CHIDB_OK;
    return CHIDB_OK;
}


int chidb_dbm_op_Null (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
     uint32_t the_reg = op->p2;
    if(!EXISTS_REGISTER(stmt,the_reg)){
        //如果寄存器数量不够，就再加一个
        chidb_dbm_register_t *temp = (chidb_dbm_register_t*)realloc(stmt->reg,sizeof(chidb_dbm_register_t)*(the_reg+1));
        stmt->reg = temp;
        stmt->nReg = the_reg;
    }
    stmt->reg[the_reg].type = REG_NULL;
    stmt->reg[the_reg].value.bin.nbytes = 0;
    stmt->reg[the_reg].value.bin.bytes = NULL;
    return CHIDB_OK;
}


int chidb_dbm_op_ResultRow (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_MakeRecord (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Insert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

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
    if(cmp!=EQ&&cmp!=NOTCMP)   
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
  fprintf(stderr,"todo: chidb_dbm_op_IdxGt\n");
  exit(1);
}

/* IdxGe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) >= k, jump
 */
int chidb_dbm_op_IdxGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxGe\n");
  exit(1);
}

/* IdxLt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) < k, jump
 */
int chidb_dbm_op_IdxLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxLt\n");
  exit(1);
}

/* IdxLe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) <= k, jump
 */
int chidb_dbm_op_IdxLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxLe\n");
  exit(1);
}


/* IdxPKey p1 p2 * *
 *
 * p1: cursor
 * p2: register
 *
 * store pkey from (cell at cursor p1) in (register at p2)
 */
int chidb_dbm_op_IdxPKey (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxKey\n");
  exit(1);
}

/* IdxInsert p1 p2 p3 *
 *
 * p1: cursor
 * p2: register containing IdxKey
 * p2: register containing PKey
 *
 * add new (IdkKey,PKey) entry in index BTree pointed at by cursor at p1
 */
int chidb_dbm_op_IdxInsert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxInsert\n");
  exit(1);
}


int chidb_dbm_op_CreateTable (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_CreateIndex (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Copy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SCopy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Halt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

