import xlrd
import datetime
import pandas as pd
import numpy as np
import io
import uuid
import time
# uuid.uuid1()　　基于MAC地址，时间戳，随机数来生成唯一的uuid，可以保证全球范围内的唯一性。
# uuid.uuid2()　　算法与uuid1相同，不同的是把时间戳的前4位置换为POSIX的UID。不过需要注意的是python中没有基于DCE的算法，所以python的uuid模块中没有uuid2这个方法。
# uuid.uuid3(namespace,name)　　通过计算一个命名空间和名字的md5散列值来给出一个uuid，所以可以保证命名空间中的不同名字具有不同的uuid，但是相同的名字就是相同的uuid了。namespace并不是一个自己手动指定的字符串或其他量，而是在uuid模块中本身给出的一些值。比如uuid.NAMESPACE_DNS，uuid.NAMESPACE_OID，uuid.NAMESPACE_OID这些值。这些值本身也是UUID对象，根据一定的规则计算得出。
# uuid.uuid4()　　通过伪随机数得到uuid，是有一定概率重复的
# uuid.uuid5(namespace,name)　　和uuid3基本相同，只不过采用的散列算法是sha1
from sqlalchemy import create_engine

def read_excel():
    
    # 打开文件git
    workbook = xlrd.open_workbook('D:/development/workspace/test/src_s/file/test.xlsx')
    # 获取所有sheet
    print(workbook.sheet_names())  # [u'sheet1', u'sheet2']
 
    # 根据sheet索引或者名称获取sheet内容
    sheet1 = workbook.sheet_by_index(0)  # sheet索引从0开始
 
    # sheet的名称，行数，列数
    print(sheet1.name, sheet1.nrows, sheet1.ncols)
 
    # 获取整行和整列的值（数组）
    rows = sheet1.row_values(3)  # 获取第四行内容
    cols = sheet1.col_values(2)  # 获取第三列内容
    print(rows)
    print(cols)
 
    # 获取单元格内容
    print(sheet1.cell(1, 0).value.encode('utf-8'))
    print(sheet1.cell_value(1, 0).encode('utf-8'))
    print(sheet1.row(1)[0].value.encode('utf-8'))
 
    # 获取单元格内容的数据类型
    print(sheet1.cell(1, 0).ctype)

def pd_read_excel():
    start_time = time.time()
    raw_data = pd.read_excel('D:/development/workspace/test/src_s/file/1M_dataset.xlsx')
    end_time = time.time()
    print ('read excel run time ={} second'.format(end_time-start_time))
    result = generate_df_uuid(raw_data)
    return result

def generate_df_uuid(df):
    #start_time = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
    #print ('start uuid time: ' + start_time)
    start_time = time.time()
    df['uuid'] = [uuid.uuid4() for x in range(len(df.index))]
    end_time = time.time()

    #end_time = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
    print ('for - loop uuid run time ={} second'.format(end_time-start_time))

    start_time = time.time()
    for index, data_row in df.iterrows():
        data_row['uuid'] = uuid.uuid4()
    end_time = time.time()
    print ('iterrows uuid run time ={} second'.format(end_time-start_time))
    return df

def write_to_table(df, table_name, if_exists='fail'):
    start_time = time.time()
    db_engine = create_engine('postgres://postgres:Welcome@pwc01@localhost:5432/test')# 初始化引擎
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
    #append：如果表存在，则将数据添加到这个表的后面
    #fail：如果表存在就不操作
    #replace：如果存在表，删了，重建
    table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
                               index=False, if_exists=if_exists,schema = 'public')
    table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY public.%s FROM STDIN HEADER DELIMITER '|' CSV" %table_name
            cursor.copy_expert(copy_cmd, string_data_io)
            connection.connection.commit()
    
    end_time = time.time()
    print ('bulk create run time ={} second'.format(end_time-start_time))
    

if __name__ == '__main__':
    data = pd_read_excel()
    #write_to_table(data,'test','replace')