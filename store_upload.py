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
path = 'store_daily/'
files = glob.glob(path + "*.xls")
print(11)
for file in files:
    try:
        # 读取 Excel 文件
        df = pd.read_excel(file, skiprows=7, dtype=str)

        df['浏览量'] = df['浏览量'].str.replace(',', '').astype(int)
        df['无线端浏览量'] = df['无线端浏览量'].str.replace(',', '').astype(int)
        df['支付金额'] = df['支付金额'].str.replace(',', '').astype(float)
        df['无线端支付金额'] = df['无线端支付金额'].str.replace(',', '').astype(float)
        df['下单金额'] = df['下单金额'].str.replace(',', '').astype(float)
        df['无线端下单金额'] = df['无线端下单金额'].str.replace(',', '').astype(float)
        df['商品浏览量'] = df['商品浏览量'].str.replace(',', '').astype(int)
        df['无线端商品浏览量'] = df['无线端商品浏览量'].str.replace(',', '').astype(int)
        df['平均支付_签收时长(秒)'] = df['平均支付_签收时长(秒)'].str.replace(',', '').astype(float)
        df['成功退款金额'] = df['成功退款金额'].str.replace(',', '').astype(float)
        df['老买家支付金额'] = df['老买家支付金额'].str.replace(',', '').astype(float)
        df['下单-支付转化率'] = df['下单-支付转化率'].str.replace('%', '').astype(float) / 100
        print(13)
        df['PC端下单-支付转化率'] = df['PC端下单-支付转化率'].str.replace('%', '').astype(float) / 100
        df['无线端下单-支付转化率'] = df['无线端下单-支付转化率'].str.replace('%', '').astype(float) / 100

        print(12)


        # 指定列名和占位符
        columns_str = ','.join(['`' + col + '`' for col in df.columns])
        placeholders = ','.join(['%s'] * len(df.columns))
        # 构造要更新的列字符串，此处假设要更新所有列
        update_str = ','.join([f"`{col}`=VALUES(`{col}`)" for col in df.columns])

        # 遍历dataframe，逐行插入到MySQL数据库中
        for index, row in df.iterrows():
            # 构造插入SQL语句和参数
            sql = f"INSERT INTO STORE_TOTAL ({columns_str}) VALUES ({placeholders}) " \
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
