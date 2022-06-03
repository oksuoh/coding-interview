import datetime
import pandas as pd
from typing import List, Dict
from itertools import groupby, chain


def split_by_vehicle_id(df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """vehicle id 별로 데이터 쪼개기"""
    df_dict = {}
    vehicle_id_list = df['vehicle_name'].unique()
    for vid in vehicle_id_list:
        new_df = df[df['vehicle_name'] == vid]
        df_dict[vid] = new_df
    assert df.shape[0] == sum(vdf.shape[0] for vdf in df_dict.values())
    return df_dict


def split_by_time_diff(df_dict: Dict[int, pd.DataFrame], 
                       sec=4) -> Dict[int, List[pd.DataFrame]]:
    """데이터 기록(datetime) 간격이 5초 이상 차이나는 부분 데이터 쪼개기"""
    new_df_dict = {}
    
    # vehicle id를 반복
    for vehicle_id, vdf in df_dict.items():
        splitted_df_list = []
        prev_index = 0
        prev_datetime = vdf.iloc[0]['datetime']
        
        # row를 반복
        for idx, row in enumerate(vdf.iloc[1:].itertuples(), 1):
            now_datetime = row.datetime
            diff = now_datetime - prev_datetime
            if diff > datetime.timedelta(seconds=sec):
                splitted_df_list.append(vdf.iloc[prev_index: idx])
                prev_index = idx
            prev_datetime = row.datetime
        splitted_df_list.append(vdf.iloc[prev_index:])
        new_df_dict[vehicle_id] = splitted_df_list
        assert vdf.shape[0] == sum(sdf.shape[0] for sdf in splitted_df_list)
    return new_df_dict


def remove_flickering(df_dict: Dict[int, List[pd.DataFrame]]) -> List[int]:
    """5초 이상 유지되지 않은 flickering하는 부분 제거"""
    all_status = []
    
    # vehicle id를 반복
    for vehicle_id, df_list in df_dict.items():
        
        # splitted df를 반복
        for datum in df_list:
            loaded = list(datum['loaded'])
            dup_cnt = [sum(1 for _ in group) for _, group in groupby(loaded)]
            dup_val = [loaded[0]]
            
            for i in range(len(dup_cnt)-1):
                if dup_val[-1] == 0:
                    dup_val.append(1)
                else:
                    dup_val.append(0)
            
            true_status = []
            for val, cnt in zip(dup_val, dup_cnt):
                if not true_status or cnt >= 5:
                    true_status.extend([val]*cnt)
                else:
                    true_status.extend([true_status[-1]]*cnt)
            all_status.append(true_status)
    return list(chain.from_iterable(all_status))


def run(data_path: str) -> List[int]:
    """전체 프로세스 실행"""
    raw_df = pd.read_csv(data_path)
    raw_df['datetime'] = pd.to_datetime(raw_df['datetime'], format='%Y-%m-%d %H:%M:%S')
    df = raw_df.sort_values(['vehicle_name', 'datetime'])    

    df_dict = split_by_vehicle_id(df)
    df_dict = split_by_time_diff(df_dict)
    result = remove_flickering(df_dict)
    # df['result'] = result
    return result


if __name__ == "__main__":
    output = run('./pose_example.csv')