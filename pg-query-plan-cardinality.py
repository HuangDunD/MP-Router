import psycopg2

# 数据库连接参数
db_params = {
    "host": "localhost",
    "port": "15432",
    "database": "template1",
    "user": "gpadmin",
    "password": "gpadmin"
}

def get_query_plan_cardinality(query, db_params):
    try:
        # 建立数据库连接
        connection = psycopg2.connect(**db_params)
        if connection is None:
            print("Failed to connect to the database.")
            return None
        else :
            print("Connected to the database.")
        # 创建一个游标对象
        cursor = connection.cursor()

        # 使用EXPLAIN命令获取查询计划
        explain_query = f"EXPLAIN {query}"
        cursor.execute(explain_query)

        # 获取查询计划结果
        plan = cursor.fetchall()

        # 解析查询计划，提取基数信息
        cardinality = None
        for row in plan:
            line = row[0]
            if 'rows=' in line:
                start_index = line.find('rows=') + len('rows=')
                end_index = line.find(' ', start_index)
                if end_index == -1:
                    end_index = len(line)
                cardinality = int(line[start_index:end_index])
                break

        # 关闭游标和连接
        cursor.close()
        connection.close()

        return cardinality

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL or getting query plan", error)
        return None

# 示例查询
query = "SELECT C.C_NAME, O.O_ORDERSTATUS FROM CUSTOMER C JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY;"

# 获取基数
cardinality = get_query_plan_cardinality(query, db_params)
if cardinality is not None:
    print(f"Estimated cardinality: {cardinality}")    