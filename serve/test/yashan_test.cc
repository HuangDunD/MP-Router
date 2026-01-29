#include <string.h>
#include <cstdio>
#include "yacli.h"

#undef YAC_CALL
#define YAC_CALL(proc)                           \
    do {                                         \
        if ((YacResult)(proc) != YAC_SUCCESS) {  \
            return YAC_ERROR;                    \
        }                                        \
    } while (0)

void printError()
{
    YacInt32   code;
    YacChar    msg[1000];
    YacTextPos pos;

    yacGetDiagRec(&code, msg, 1000, NULL, NULL, 0, &pos);
    if (pos.line != 0) {
        printf("[%d:%d]", pos.line, pos.column);
    }
    printf("YAC-%05d %s\n", code, msg);
}

typedef struct {
    YacHandle env;
    YacHandle conn;
    YacHandle stmt;
} YacTestEnv;

YacTestEnv gTestEnv = { 0 };

// C驱动常用操作：

// 1、连接数据库
YacResult testConnect()
{
    //更改为实际数据库服务器的IP和端口
    const YacChar* gSrvStr = "10.10.2.35:1688,10.10.2.36:1688,10.10.2.39:1688,10.10.2.40:1688";
    const YacChar* user = "sys";
    const YacChar* pwd = "Rdjc#2025";

    YAC_CALL(yacAllocHandle(YAC_HANDLE_ENV, NULL, &gTestEnv.env));
    YAC_CALL(yacAllocHandle(YAC_HANDLE_DBC, gTestEnv.env, &gTestEnv.conn));
    YAC_CALL(yacConnect(gTestEnv.conn, gSrvStr, YAC_NULL_TERM_STR, user, YAC_NULL_TERM_STR, pwd, YAC_NULL_TERM_STR));
    YAC_CALL(yacAllocHandle(YAC_HANDLE_STMT, gTestEnv.conn, &gTestEnv.stmt));

    return YAC_SUCCESS;
}

// 2、断开数据库
YacResult testDisConnect()
{
    YAC_CALL(yacFreeHandle(YAC_HANDLE_STMT, gTestEnv.stmt));
    yacDisconnect(gTestEnv.conn);
    YAC_CALL(yacFreeHandle(YAC_HANDLE_DBC, gTestEnv.conn));
    YAC_CALL(yacFreeHandle(YAC_HANDLE_ENV, gTestEnv.env));

    return YAC_SUCCESS;
}

// 3、单行绑定导入数据
YacResult testSingleBind()
{
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "drop table if exists test_yacli", YAC_NULL_TERM_STR));
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "create table test_yacli(col1 int, col2 varchar(200))", YAC_NULL_TERM_STR));

    YAC_CALL(yacPrepare(gTestEnv.stmt, "insert into test_yacli values(?, ?)", YAC_NULL_TERM_STR));

    YacInt32 inputInt;
    YacChar  inputVarchar[200];
    YacInt32 indicator;
    YAC_CALL(yacBindParameter(gTestEnv.stmt, 1, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, &inputInt, sizeof(YacInt32), sizeof(YacInt32), NULL));
    YAC_CALL(yacBindParameter(gTestEnv.stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_VARCHAR2, inputVarchar, 200, 200, &indicator));

    /* insert data
    99,  '0123456789'
    100, '9876543210'
    */
    inputInt = 99;
    memcpy(inputVarchar, "0123456789", 10);
    indicator = 10;
    YAC_CALL(yacExecute(gTestEnv.stmt));

    inputInt = 100;
    memcpy(inputVarchar, "98765", 10);
    indicator = 5;
    YAC_CALL(yacExecute(gTestEnv.stmt));

    YAC_CALL(yacCommit(gTestEnv.conn));

    return YAC_SUCCESS;
}

// 4、单行取数据
YacResult testSingleFetch()
{
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "select * from test_yacli", YAC_NULL_TERM_STR));

    YacInt32 outputInt;
    YacChar  outputVarchar[200];
    YacInt32 indicator;
    YAC_CALL(yacBindColumn(gTestEnv.stmt, 0, YAC_SQLT_INTEGER, &outputInt, sizeof(YacInt32), NULL));
    YAC_CALL(yacBindColumn(gTestEnv.stmt, 1, YAC_SQLT_VARCHAR2, outputVarchar, 200, &indicator));

    /* fetch data
    99,  '0123456789'
    100, '9876543210'
    */
    YacUint32 fetchedRows;
    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    if (fetchedRows != 1 || outputInt != 99 || memcmp(outputVarchar, "0123456789", indicator) != 0 || indicator != 10)
    {
        return YAC_ERROR;
    }

    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    if (fetchedRows != 1 || outputInt != 100 || memcmp(outputVarchar, "9876543210", indicator) != 0 || indicator != 5)
    {
        return YAC_ERROR;
    }

    return YAC_SUCCESS;
}

// 5、批量绑定导入数据
YacResult testBatchBind()
{
    YacUint32 paramSet = 10;
    YAC_CALL(yacSetStmtAttr(gTestEnv.stmt, YAC_ATTR_PARAMSET_SIZE, &paramSet, sizeof(YacUint32)));
    // 设置批量绑定行数为10

    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "drop table if exists test_yacli", YAC_NULL_TERM_STR));
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "create table test_yacli(col1 int, col2 varchar(200))", YAC_NULL_TERM_STR));

    YAC_CALL(yacPrepare(gTestEnv.stmt, "insert into test_yacli values(?, ?)", YAC_NULL_TERM_STR));

    YacInt32 inputInt[10];
    YacChar  inputVarchar[10][200];
    YacInt32 indicator[10];
    YAC_CALL(yacBindParameter(gTestEnv.stmt, 1, YAC_PARAM_INPUT, YAC_SQLT_INTEGER, inputInt, sizeof(YacInt32), sizeof(YacInt32), NULL));
    YAC_CALL(yacBindParameter(gTestEnv.stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_VARCHAR2, (YacPointer)inputVarchar, 200, 200, indicator));

    /*insert data
    batch insert 1
    1,   '0123456789'
    2,   '0123456789'
    3,   '0123456789'
    4,   '0123456789'
    5,   '0123456789'
    6,   '0123456789'
    7,   '0123456789'
    8,   '0123456789'
    9,   '0123456789'
    10,  '0123456789'
    batch insert 2
    11,  '9876543210'
    12,  '9876543210'
    13,  '9876543210'
    14,  '9876543210'
    15,  '9876543210'
    16,  '9876543210'
    17,  '9876543210'
    18,  '9876543210'
    19,  '9876543210'
    20,  '9876543210'
    */
    for (YacInt32 i = 0; i < 10; i++)
    {
        inputInt[i] = i + 1;
        memcpy(inputVarchar[i], "0123456789", 10);
        indicator[i] = 10;
    }
    YAC_CALL(yacExecute(gTestEnv.stmt));

    for (YacInt32 i = 0; i < 10; i++)
    {
        inputInt[i] = i + 11;
        memcpy(inputVarchar[i], "9876543210", 10);
        indicator[i] = 10;
    }
    YAC_CALL(yacExecute(gTestEnv.stmt));

    YAC_CALL(yacCommit(gTestEnv.conn));

    paramSet = 1;
    YAC_CALL(yacSetStmtAttr(gTestEnv.stmt, YAC_ATTR_PARAMSET_SIZE, &paramSet, sizeof(YacUint32)));
    // 设置批量绑定行数为1

    return YAC_SUCCESS;
}

// 6、批量取数据
YacResult testBatchFetch()
{
    YacUint32 rowSet = 10;
    YAC_CALL(yacSetStmtAttr(gTestEnv.stmt, YAC_ATTR_ROWSET_SIZE, &rowSet, sizeof(YacUint32)));
    // 设置批量取数据绑定行数为10

    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "select * from test_yacli", YAC_NULL_TERM_STR));

    YacInt32 outputInt[10];
    YacChar  outputVarchar[10][200];
    YacInt32 indicator[10];
    YAC_CALL(yacBindColumn(gTestEnv.stmt, 0, YAC_SQLT_INTEGER, outputInt, sizeof(YacInt32), NULL));
    YAC_CALL(yacBindColumn(gTestEnv.stmt, 1, YAC_SQLT_VARCHAR2, (YacPointer)outputVarchar, 200, indicator));

    /*fetch data
    batch fetch 1
    1,   '0123456789'
    2,   '0123456789'
    3,   '0123456789'
    4,   '0123456789'
    5,   '0123456789'
    6,   '0123456789'
    7,   '0123456789'
    8,   '0123456789'
    9,   '0123456789'
    10,  '0123456789'
    batch fetch 2
    11,  '9876543210'
    12,  '9876543210'
    13,  '9876543210'
    14,  '9876543210'
    15,  '9876543210'
    16,  '9876543210'
    17,  '9876543210'
    18,  '9876543210'
    19,  '9876543210'
    20,  '9876543210'
    */
    YacUint32 fetchedRows;
    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    if (fetchedRows != 10)
    {
        return YAC_ERROR;
    }
    for (YacInt32 i = 0; i < 10; i++)
    {
        if (outputInt[i] != i + 1 || memcmp(outputVarchar[i], "0123456789", indicator[i]) != 0 || indicator[i] != 10)
        {
            return YAC_ERROR;
        }
    }

    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    if (fetchedRows != 10)
    {
        return YAC_ERROR;
    }
    for (YacInt32 i = 0; i < 10; i++)
    {
        if (outputInt[i] != i + 11 || memcmp(outputVarchar[i], "9876543210", indicator[i]) != 0 || indicator[i] != 10)
        {
            return YAC_ERROR;
        }
    }

    rowSet = 1;
    YAC_CALL(yacSetStmtAttr(gTestEnv.stmt, YAC_ATTR_ROWSET_SIZE, &rowSet, sizeof(YacUint32)));
    // 设置批量取数据绑定行数为1

    return YAC_SUCCESS;
}

// 7、单行绑定LOB导入数据
YacResult testSingleBindLob()
{
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "drop table if exists test_yacli", YAC_NULL_TERM_STR));
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "create table test_yacli(col1 clob, col2 blob)", YAC_NULL_TERM_STR));

    YAC_CALL(yacPrepare(gTestEnv.stmt, "insert into test_yacli values(?, ?)", YAC_NULL_TERM_STR));

    YacLobLocator* lobLocator1;
    YacLobLocator* lobLocator2;

    YAC_CALL(yacBindParameter(gTestEnv.stmt, 1, YAC_PARAM_INPUT, YAC_SQLT_CLOB, &lobLocator1, 0, 0, NULL));
    YAC_CALL(yacBindParameter(gTestEnv.stmt, 2, YAC_PARAM_INPUT, YAC_SQLT_BLOB, &lobLocator2, 0, 0, NULL));

    /* insert lob data
    '01234567899876543210',  'aaaaaaaaaabbbbbbbbbb'
    '98765432100123456789',  'bbbbbbbbbbaaaaaaaaaa'
    */

    // first row insert
    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_CLOB, (YacVoid**)&lobLocator1));
    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_BLOB, (YacVoid**)&lobLocator2));
    YAC_CALL(yacLobCreateTemporary(gTestEnv.conn, lobLocator1));
    YAC_CALL(yacLobCreateTemporary(gTestEnv.conn, lobLocator2));
    YacChar buf1[100] = "01234567899876543210";
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator1, NULL, (YacUint8*)buf1, 10));
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator1, NULL, (YacUint8*)(buf1 + 10), 10));
    YacChar buf2[100] = "aaaaaaaaaabbbbbbbbbb";
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator2, NULL, (YacUint8*)buf2, 10));
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator2, NULL, (YacUint8*)(buf2 + 10), 10));
    YAC_CALL(yacExecute(gTestEnv.stmt));
    YAC_CALL(yacLobFreeTemporary(gTestEnv.conn, lobLocator1));
    YAC_CALL(yacLobFreeTemporary(gTestEnv.conn, lobLocator2));
    YAC_CALL(yacLobDescFree(lobLocator1, YAC_TYPE_CLOB));
    YAC_CALL(yacLobDescFree(lobLocator2, YAC_TYPE_BLOB));

    // second row insert
    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_CLOB, (YacVoid**)&lobLocator1));
    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_BLOB, (YacVoid**)&lobLocator2));
    YAC_CALL(yacLobCreateTemporary(gTestEnv.conn, lobLocator1));
    YAC_CALL(yacLobCreateTemporary(gTestEnv.conn, lobLocator2));
    YacChar buf3[100] = "98765432100123456789";
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator1, NULL, (YacUint8*)buf3, 10));
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator1, NULL, (YacUint8*)(buf3 + 10), 10));
    YacChar buf4[100] = "bbbbbbbbbbaaaaaaaaaa";
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator2, NULL, (YacUint8*)buf4, 10));
    YAC_CALL(yacLobWrite(gTestEnv.conn, lobLocator2, NULL, (YacUint8*)(buf4 + 10), 10));
    YAC_CALL(yacExecute(gTestEnv.stmt));
    YAC_CALL(yacLobFreeTemporary(gTestEnv.conn, lobLocator1));
    YAC_CALL(yacLobFreeTemporary(gTestEnv.conn, lobLocator2));
    YAC_CALL(yacLobDescFree(lobLocator1, YAC_TYPE_CLOB));
    YAC_CALL(yacLobDescFree(lobLocator2, YAC_TYPE_BLOB));

    YAC_CALL(yacCommit(gTestEnv.conn));

    return YAC_SUCCESS;
}

// 8、单行取LOB数据
YacResult testSingleFetchLob()
{
    YAC_CALL(yacDirectExecute(gTestEnv.stmt, "select * from test_yacli", YAC_NULL_TERM_STR));

    YacLobLocator* lobLocator1;
    YacLobLocator* lobLocator2;

    YAC_CALL(yacBindColumn(gTestEnv.stmt, 0, YAC_SQLT_CLOB, &lobLocator1, 0, NULL));
    YAC_CALL(yacBindColumn(gTestEnv.stmt, 1, YAC_SQLT_BLOB, &lobLocator2, 0, NULL));

    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_CLOB, (YacVoid**)&lobLocator1));
    YAC_CALL(yacLobDescAlloc(gTestEnv.conn, YAC_TYPE_BLOB, (YacVoid**)&lobLocator2));

    /* fetch lob data
    '01234567899876543210',  'aaaaaaaaaabbbbbbbbbb'
    '98765432100123456789',  'bbbbbbbbbbaaaaaaaaaa'
    */

    YacUint32 fetchedRows;
    YacChar buf1[100];
    YacUint64 bytes1 = 100;
    YacChar buf2[100];
    YacUint64 bytes2 = 100;

    // first row fetch
    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    YAC_CALL(yacLobRead(gTestEnv.conn, lobLocator1, &bytes1, (YacUint8*)buf1, 0));
    YAC_CALL(yacLobRead(gTestEnv.conn, lobLocator2, &bytes2, (YacUint8*)buf2, 0));
    if (fetchedRows != 1 || memcmp(buf1, "01234567899876543210", bytes1) != 0 || memcmp(buf2, "aaaaaaaaaabbbbbbbbbb", bytes2) != 0)
    {
        return YAC_ERROR;
    }

    // second row fetch
    YAC_CALL(yacFetch(gTestEnv.stmt, &fetchedRows));
    YAC_CALL(yacLobRead(gTestEnv.conn, lobLocator1, &bytes1, (YacUint8*)buf1, 0));
    YAC_CALL(yacLobRead(gTestEnv.conn, lobLocator2, &bytes2, (YacUint8*)buf2, 0));
    if (fetchedRows != 1 || memcmp(buf1, "98765432100123456789", bytes1) != 0 || memcmp(buf2, "bbbbbbbbbbaaaaaaaaaa", bytes2) != 0)
    {
        return YAC_ERROR;
    }

    YAC_CALL(yacLobDescFree(lobLocator1, YAC_TYPE_CLOB));
    YAC_CALL(yacLobDescFree(lobLocator2, YAC_TYPE_BLOB));

    return YAC_SUCCESS;
}

YacResult cexample()
{
    YAC_CALL(testConnect());
    YAC_CALL(testSingleBind());
    YAC_CALL(testSingleFetch());
    YAC_CALL(testBatchBind());
    YAC_CALL(testBatchFetch());
    YAC_CALL(testSingleBindLob());
    YAC_CALL(testSingleFetchLob());
    YAC_CALL(testDisConnect());

    return YAC_SUCCESS;
}

int main()
{
    if (cexample() == YAC_SUCCESS) {
        printf("cexample succeed!\n");
    } else {
        printError();
        printf("cexample failed!\n");
    }
    return 0;
}

// #include <string.h>
// #include "yacli.h"

// #define YAC_CALL(proc)                           \
//     do {                                         \
//         if ((YacResult)(proc) != YAC_SUCCESS) {  \
//             return YAC_ERROR;                    \
//         }                                        \
//     } while (0)

// void printError()
// {
//     YacInt32   code;
//     YacChar    msg[1000];
//     YacTextPos pos;

//     yacGetDiagRec(&code, msg, 1000, NULL, NULL, 0, &pos);
//     if (pos.line != 0) {
//         printf("[%d:%d]", pos.line, pos.column);
//     }
//     printf("YAC-%05d %s\n", code, msg);
// }

// typedef struct {
//     YacHandle env;
//     YacHandle conn;
//     YacHandle stmt;
// } YacTestEnv;

// YacTestEnv gTestEnv = { 0 };

// // 连接数据库
// YacResult testConnect()
// {
//     // 更改为实际数据库服务器的IP和端口
//     const YacChar* gSrvStr = "10.10.2.35:1688\0";
//     const YacChar* user = "sys\0";
//     // Shell 解析陷阱: "Rdic12#$2025" 在 shell 中被解析为 "Rdic12#025" ($2 为空)
//     // 用户实际使用的有效密码是 "Rdic12#025"
//     const YacChar* pwd = "Rdic12#025\0";

//     YAC_CALL(yacAllocHandle(YAC_HANDLE_ENV, NULL, &gTestEnv.env));
//     YAC_CALL(yacAllocHandle(YAC_HANDLE_DBC, gTestEnv.env, &gTestEnv.conn));
//     YAC_CALL(yacConnect(gTestEnv.conn, gSrvStr, YAC_NULL_TERM_STR, user, YAC_NULL_TERM_STR, pwd, YAC_NULL_TERM_STR));
//     YAC_CALL(yacAllocHandle(YAC_HANDLE_STMT, gTestEnv.conn, &gTestEnv.stmt));

//     return YAC_SUCCESS;
// }
// // 断开数据库
// YacResult testDisConnect()
// {
//     YAC_CALL(yacFreeHandle(YAC_HANDLE_STMT, gTestEnv.stmt));
//     yacDisconnect(gTestEnv.conn);
//     YAC_CALL(yacFreeHandle(YAC_HANDLE_DBC, gTestEnv.conn));
//     YAC_CALL(yacFreeHandle(YAC_HANDLE_ENV, gTestEnv.env));

//     return YAC_SUCCESS;
// }

// YacResult cexample()
// {
//     YAC_CALL(testConnect());
//     YAC_CALL(testDisConnect());

//     return YAC_SUCCESS;
// }

// int main()
// {
//     if (cexample() == YAC_SUCCESS) {
//         printf("cexample succeed!\n");
//     } else {
//         printError();
//         printf("cexample failed!\n");
//     }
//     return 0;
// }
