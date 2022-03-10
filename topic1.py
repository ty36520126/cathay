import pandas as pd

df_a = pd.read_csv("a_lvr_land_a.csv")
df_b = pd.read_csv("b_lvr_land_a.csv")
df_c = pd.read_csv("e_lvr_land_a.csv")
df_f = pd.read_csv("f_lvr_land_a.csv")
df_h = pd.read_csv("h_lvr_land_a.csv")

df_a.drop([0], inplace=True)
df_b.drop([0], inplace=True)
df_c.drop([0], inplace=True)
df_f.drop([0], inplace=True)
df_h.drop([0], inplace=True)

df_a['縣市'] = '台北市'
df_b['縣市'] = '臺中市'
df_c['縣市'] = '高雄市'
df_f['縣市'] = '新北市'
df_h['縣市'] = '桃園市'

df_all = pd.concat([df_a, df_b, df_c, df_f, df_h])
cols = df_all.columns.tolist()
cols = cols[-1:] + cols[:-1]
df_all = df_all[cols]

for i in [df_a, df_b, df_c, df_f, df_h, df_all]:
    print(i.shape)


#================================================================
#                            filter_a
#================================================================
number_map = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '零': 0}
unit_map = {"十": 10,"百": 100,"千": 1000,"萬": 10000,"億": 100000000}
def cn2num(inputs):
    output = 0
    unit = 1
    num = 0
    for index, cn_num in enumerate(inputs):
        
        if (index==0) & (cn_num in unit_map):
            num=1
        
        if cn_num in number_map:
        # 數字
            num = number_map[cn_num]
        # 最後的個位數字
        if index == len(inputs) - 1:
            output = output + num
        elif cn_num in unit_map:
        # 單位
            unit = unit_map[cn_num]
            output = output + num * unit

    return (output)

df_all['總樓層數-轉換'] = df_all['總樓層數'].replace(regex='(?=層)(.*)', value='')
df_all['總樓層數-轉換'] = df_all['總樓層數-轉換'].apply(lambda x : cn2num(str(x)) )

c1 = df_all['主要用途'] == '住家用'
c2 = df_all['建物型態'].str.contains('住宅大樓', regex=True)
c3 = df_all['總樓層數-轉換'] >= 13
filter_a = df_all[c1 & c2 & c3]
filter_a.head()


#================================================================
#                            filter_b
#================================================================

df_all['車位數'] = df_all['交易筆棟數'].replace(regex='(.*)(?<=車位)', value='').astype(float, errors = 'raise')
df_all['總價元'] = df_all['總價元'].astype('float')
df_all['車位總價元'] = df_all['車位總價元'].astype('float')

trans_c = df_all.shape[0]
parking_c = sum(df_all['車位數'])
avg_trans_p = sum(df_all['總價元']) / trans_c
avg_parking_p = sum(df_all['車位總價元']) / parking_c

filter_b = pd.DataFrame(
    data ={"總件數" : [trans_c],
         "總車位數": [parking_c],
         "平均總價元": [avg_trans_p],
         "平均車位總價元": [avg_parking_p]})

filter_b  