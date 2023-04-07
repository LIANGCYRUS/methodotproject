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
path = 'sku_daily/'
files = glob.glob(path + "*.xls")

# 读取所有Excel文件并进行合并
dfs = []

for file in files:
    df = pd.read_excel(file, skiprows=7)
    print(df)
    dfs.append(df)

merged_df = pd.concat(dfs)

sku_name = [
    [702634957656, 'SW19 DISCOVERY 6AM', 'PARFUM'],
    [702651213551, 'SW19 6AM EAU DE PARFUM', 'PARFUM'],
    [703214891276, 'SW19 DISCOVERY NOON', 'PARFUM'],
    [703216107314, 'SW19 NOON EAU DE PARFUM', 'PARFUM'],
    [703217415404, 'SW19 DISCOVERY 3PM', 'PARFUM'],
    [702980074586, 'SW19 3PM EAU DE PARFUM', 'PARFUM'],
    [702754645600, 'SW19 DISCOVERY 9PM', 'PARFUM'],
    [702463824208, 'SW19 9PM EAU DE PARFUM', 'PARFUM'],
    [702983226146, 'SW19 DISCOVERY MIDNIGHT', 'PARFUM'],
    [702994978096, 'SW19 MIDNIGHT EAU DE PARFUM', 'PARFUM'],
    [703235867345, 'SW19 6AM HAND CREAM', 'HANDCARE'],
    [702485828777, 'SW19 NOON HAND CREAM', 'HANDCARE'],
    [703005686832, 'SW19 3PM HAND CREAM', 'HANDCARE'],
    [703006678694, 'SW19 9PM HAND CREAM', 'HANDCARE'],
    [702488404102, 'SW19 MIDNIGHT HAND CREAM', 'HANDCARE'],
    [703728620404, 'SW19 3PM BODY WASH', 'BODYCARE'],
    [704480163071, 'SW19 MIDNIGHT BODY WASH', 'BODYCARE'],
    [704020985509, 'SW19 3PM BODY LOTION', 'BODYCARE'],
    [703733472221, 'SW19 MIDNIGHT BODY LOTION', 'BODYCARE'],
    [704022669390, 'SW19 6AM PILLOW MIST', 'OTHER'],
    [704251454139, 'SW19 3PM PILLOW MIST', 'OTHER'],
    [703246671776, 'SW19 HANDCREAM MINI SET', 'HANDCARE'],
    [703250831614, 'SW19 DISCOVERY SET', 'PARFUM'],
    [704282859185, 'SW19 PERFUME SAMPLE KIT', 'PARFUM'],
    [703824125409, 'SW19 3PM BODY SAMPLE KIT', 'BODYCARE'],
    [703824565769, 'SW19 MIDNIGHT BODY SAMPLE KIT''BODYCARE'],
    [704051442837, 'SW19 6AM HAND CREAM MINI (10ML)', 'HANDCARE'],
    [703825757036, 'SW19 NOON HAND CREAM MINI (10ML)', 'HANDCARE'],
    [704285935181, 'SW19 3PM HAND CREAM MINI (10ML)', 'HANDCARE'],
    [703826021291, 'SW19 9PM HAND CREAM MINI (10ML)', 'HANDCARE'],
    [704052590701, 'SW19 MIDNIGHT HAND CREAM MINI (10ML)', 'HANDCARE']
]
sku_name_df = pd.DataFrame(sku_name, columns=['商品ID', 'en', 'category'])

name_merger = pd.merge(merged_df, sku_name_df, left_on='商品ID', right_on='商品ID')

merged_df.to_excel('merged_df.xlsx', index=False)
name_merger.to_excel('name_merger.xlsx', index=False)

merged_df = pd.read_excel('name_merger.xlsx')

print(merged_df)

# merged_df['无线端浏览量'] = merged_df['无线端浏览量'].str.replace(',', '')
# merged_df['PC端平均停留时长'] = merged_df['PC端平均停留时长'].str.replace(',', '').astype(int)

print(merged_df['PC端平均停留时长'])

# merged_df['支付金额'] = merged_df['支付金额'].str.replace(',', '').fillna(0)
# merged_df['无线端支付金额'] = merged_df['无线端支付金额'].str.replace(',', '').fillna(0)
# merged_df['下单金额'] = merged_df['下单金额'].str.replace(',', '').fillna(0)
#
merged_df['详情页跳出率'] = merged_df['详情页跳出率'].str.replace('%', '').astype(float) / 100
merged_df['PC端详情页跳出率'] = merged_df['PC端详情页跳出率'].str.replace('%', '').astype(float) / 100
merged_df['无线端详情页跳出率'] = merged_df['无线端详情页跳出率'].str.replace('%', '').astype(float) / 100
merged_df['支付转化率'] = merged_df['支付转化率'].str.replace('%', '').astype(float) / 100
merged_df['PC端支付转化率'] = merged_df['PC端支付转化率'].str.replace('%', '').astype(float) / 100
merged_df['无线端支付转化率'] = merged_df['无线端支付转化率'].str.replace('%', '').astype(float) / 100

# 将数据插入到MySQL数据库中
try:
    # 指定列名和占位符
    columns_str = ','.join(['`' + col + '`' for col in merged_df.columns])
    placeholders = ','.join(['%s'] * len(merged_df.columns))
    # # 构造要更新的列字符串，此处假设要更新所有列
    # update_str = ','.join([f"`{col}`=VALUES(`{col}`)" for col in merged_df.columns])

    # 遍历dataframe，逐行插入到MySQL数据库中
    for index, row in merged_df.iterrows():
        # 构造插入SQL语句和参数
        sql = f"INSERT INTO SKU_TOTAL ({columns_str}) VALUES ({placeholders}) "  # \
        # f"ON DUPLICATE KEY UPDATE {update_str}"
        val = tuple(row)

        # 执行SQL语句
        cursor.execute(sql, val)

    # 提交事务
    conn.commit()
except Exception as e:
    # 如果出现错误，回滚事务并打印出错信息
    conn.rollback()
    print(f"Error occured while processing row {index + 1} in file {file}: {e}")
    print(f"Problematic data: {row}")

# 关闭光标和数据库连接
cursor.close()
conn.close()
