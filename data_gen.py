import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 2024년 날짜 생성
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
date_range = pd.date_range(start=start_date, end=end_date)

# 주말 제외
weekdays = date_range[date_range.dayofweek < 5]

# 공휴일 제외 (간단한 예시)
holidays = ['2024-01-01', '2024-03-01', '2024-05-05', '2024-08-15', '2024-10-03', '2024-12-25']
workdays = weekdays[~weekdays.isin(pd.to_datetime(holidays))]

# 데이터 프레임 생성
records = []
record_idx = 507
user_idx = 2

#빈도 조절을 위한 변수
ab_probability = 1/len(workdays) #정상:1
vc_probability = 10/len(workdays) #정상:10
bt_probability = 2/len(workdays) #정상:2 비정상:10
la_probability = 0.05 #나머지는 na prob / 정상:0.05 비정상:0.15 0.18
ot_probability = 0.03 #정상:0.03 비정상:0.2
otel_probability = 0.3 #나머지는 otrt prob / 정상:0.3 비정상:0.5
el_probability = 0.03 #외출없이 조퇴 / 정상:0.03 비정상:0.1
ll_probability = 0.1  # 야근 / 정상:0.1 비정상:0.3

for date in workdays:
    # 특별한 상태 (출장, 결근, 휴가) 처리
    if np.random.rand() < ab_probability:
        records.append([record_idx, user_idx, 'AB', date + timedelta(hours=18)])
        record_idx += 1
        continue
    elif np.random.rand() < vc_probability:
        records.append([record_idx, user_idx, 'VC', date + timedelta(hours=18)])
        record_idx += 1
        continue
    elif np.random.rand() < bt_probability:
        records.append([record_idx, user_idx, 'BT', date + timedelta(hours=18)])
        record_idx += 1
        continue

    # 출근 처리
    checkin_time = date + timedelta(hours=8, minutes=np.random.randint(30, 60))
    if checkin_time.time() <= datetime.strptime('09:00', '%H:%M').time() and np.random.rand() > la_probability:
        status = 'NA'
    else:
        status = 'LA'
        checkin_time = date + timedelta(hours=9, minutes=np.random.randint(1, 30))
    records.append([record_idx, user_idx, status, checkin_time])
    record_idx += 1

    # 외출 처리
    if np.random.rand() < ot_probability:
        out_time = date + timedelta(hours=np.random.randint(11, 15), minutes=np.random.randint(0, 60))
        records.append([record_idx, user_idx, 'OT', out_time])
        record_idx += 1

        # 조퇴 또는 복귀 처리
        if np.random.rand() < otel_probability:
            el_time = out_time + timedelta(hours=np.random.randint(1, 3))
            records.append([record_idx, user_idx, 'EL', el_time])
            record_idx += 1
            continue  # 해당 날의 기록 종료
        else:
            in_time = out_time + timedelta(hours=1, minutes=np.random.randint(0, 60))
            records.append([record_idx, user_idx, 'RT', in_time])
            record_idx += 1

    # 퇴근 처리
    if np.random.rand() < el_probability:
        el_time = date + timedelta(hours=np.random.randint(15, 18), minutes=np.random.randint(0, 60))
        records.append([record_idx, user_idx, 'EL', el_time])
    elif np.random.rand() < ll_probability:
        ll_time = date + timedelta(hours=19, minutes=np.random.randint(0, 90))
        records.append([record_idx, user_idx, 'LL', ll_time])
    else:
        nl_time = date + timedelta(hours=18, minutes=np.random.randint(0, 30))
        records.append([record_idx, user_idx, 'NL', nl_time])
    record_idx += 1

# CSV 파일로 저장
output_df = pd.DataFrame(records, columns=['ATTEND_RECORD_IDX', 'USER_IDX', 'ATTEND_STATUS', 'REG_DATE'])
output_df.to_csv('data/la_ll.csv', index=False)
print(output_df)

print('CSV 파일 생성 완료')
print(output_df.head(10))