import os
import pandas as pd


def pf_value_filter(id_list_df):
    # 按照时间排序
    id_list_df = id_list_df.sort_values('time', inplace=False, ascending=True)
    time_list = id_list_df['time'].unique()
    # print(time_list)

    pf_value = []
    for current_time in tqdm(time_list):
        f_in_time = id_list_df[(id_list_df['time'] == current_time) & (id_list_df['labname'] == 'FiO2')]
        p_in_time = id_list_df[(id_list_df['time'] == current_time) & (id_list_df['labname'] == 'paO2')]
        # print(round(p_in_time.loc[p_in_time.index[0], 'value'] / f_in_time.loc[f_in_time.index[0], 'value'], 1))
        pf_value.append(
            round(p_in_time.loc[p_in_time.index[0], 'value'] / f_in_time.loc[f_in_time.index[0], 'value'], 1))
    # print(pf_value)

    valid_time = 0
    start_time = time_list[0]

    for pf, pf_time in zip(pf_value, time_list):
        print(str(pf_time) + '--' + str(pf))
        # p/f 小于300
        if pf < 300:
            valid_time = valid_time + (pf_time - start_time)
            start_time = pf_time
            print(valid_time)
            if valid_time >= 480:
                return True
        else:
            valid_time = 0
            start_time = pf_time
    return False

def merge_csv(path):
    file_list = []
    for file in os.listdir(path):
        # print(file)
        df = pd.read_csv(path + file)
        file_list.append(df)
    result = pd.concat(file_list)  # 合并文件
    result.to_csv('./file/merge_valid_id_after_pf_filter.csv', index=True, encoding='gbk')  # 保存合并后的文件

if __name__ == '__main__':
    # data = pd.read_csv('./file/test.csv')
    # print(pf_value_filter(data))
    merge_csv('./file/valid_id/')
    valid_id_df_after_pf_filter = pd.read_csv('./file/merge_valid_id_after_pf_filter.csv')
