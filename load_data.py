import psycopg2

# 数据库连接参数
db_params = {
    "host": "localhost",
    "port": "15432",
    "database": "template1",
    "user": "gpadmin",
    "password": "gpadmin"
}

# 表名和对应的 .tbl 文件列表
tables = {
    "nation": "nation.tbl",
    "region": "region.tbl",
    "customer": "customer.tbl",
    "part": "part.tbl",
    "supplier": "supplier.tbl",
    "partsupp": "partsupp.tbl",
    "orders": "orders.tbl",
    "lineitem": "lineitem.tbl"
}

try:
    # 建立数据库连接
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    # 建立数据库连接
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    for table, file in tables.items():
        file_path = f"/tmp/data/{file}"
        # 使用COPY命令导入数据
        copy_query = f"COPY {table} FROM '{file_path}' DELIMITER '|'"
        cursor.execute(copy_query)
        connection.commit()
        print(f"Data imported into {table} successfully.")

except (Exception, psycopg2.Error) as error:
    print("Error while importing data", error)
finally:
    # 关闭游标和连接
    if cursor:
        cursor.close()
    if connection:
        connection.close()