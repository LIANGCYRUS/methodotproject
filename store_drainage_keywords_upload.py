import pandas as pd
import pymysql.cursors
import glob

# 数据库连接配置
conn = pymysql.connect(
    host='sw19db.c.methodot.com',
    user='root',
    password='sw19',
    db='TMALL',
    port=33133,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# 获取光标
cursor = conn.cursor()

# 指定xls文件所在目录，遍历该目录下所有的xls文件
path = 'store_drainage_keywords_files/'
files = glob.glob(path + "*.xls")

for file in files:
    try:
        # 读取 Excel 文件
        df = pd.read_excel(file, skiprows=7)

        # df['支付金额'] = df['支付金额'].str.replace(',', '')
        df['引导支付金额'] = df['引导支付金额'].str.replace(',', '')
        #
        #
        #
        df['跳失率'] = df['跳失率'].str.replace('%', '').astype(float) / 100
        df['下单转化率'] = df['下单转化率'].str.replace('%', '').astype(float) / 100

        # 指定列名和占位符
        columns_str = ','.join(['`' + col + '`' for col in df.columns])
        placeholders = ','.join(['%s'] * len(df.columns))
        # 构造要更新的列字符串，此处假设要更新所有列
        update_str = ','.join([f"`{col}`=VALUES(`{col}`)" for col in df.columns])

        # 遍历dataframe，逐行插入到MySQL数据库中
        for index, row in df.iterrows():
            # 构造插入SQL语句和参数
            sql = f"INSERT INTO STORE_DRAINAGE_KEYWORDS ({columns_str}) VALUES ({placeholders}) " \
                  f"ON DUPLICATE KEY UPDATE {update_str}"
            val = tuple(row)

            # 执行SQL语句
            cursor.execute(sql, val)

        # 提交事务
        conn.commit()
    except Exception as e:
        # 如果出现错误，回滚事务
        conn.rollback()
        print(f"Error occured while processing file {file}: {e}")

# 关闭光标和数据库连接
cursor.close()
conn.close()