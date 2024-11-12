import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import pymysql
import sys
import google.generativeai as genai
import os
from dotenv import load_dotenv
load_dotenv()

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key = GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

user_id = 2 #직원 id

try:
    conn = pymysql.connect(
        host = os.getenv('DB_HOST'),
        port = 3308,
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        db = os.getenv('DB_NAME'),
        charset = 'utf8mb4'
    )
except pymysql.Error as e:
    print("Error connecting to MariaDB")
    sys.exit(1)

cur = conn.cursor()

query = """
SELECT ATTEND_RECORD_IDX, USER_IDX, ATTEND_STATUS, REG_DATE 
FROM jsol_attendance_record 
WHERE USER_IDX = %s
""" #일정 기간부터 조회할 수 있도록 추후 수정
cur.execute(query, (user_id,))
data = cur.fetchall()

df = pd.DataFrame(data, columns=['ATTEND_RECORD_IDX', 'USER_IDX', 'ATTEND_STATUS', 'REG_DATE'])

dict = df['ATTEND_STATUS'].value_counts().to_dict()

##############################################################

#변수
late_arrive = 0.15
late_leave = 0.28
early_leave = 0.08
out = 0.15
business_trip = 10

##############################################################

#유형분류
result = []

normalAL = dict['NA'] + dict['LA'] #dict['NL'] + dict['LL'] + dict['EL']와 같아야함

if dict['LA'] >= normalAL * late_arrive:
    result.append('LA')

if dict['LL'] >= normalAL * late_leave:
    result.append('LL')

if dict['BT'] >= business_trip:
    result.append('BT')

if dict['EL'] >= normalAL * early_leave:
    result.append('EL')

if dict['OT'] >= normalAL * out:
    result.append('OT')

print(result)

##############################################################

#리포트 생성
prompt = []
if result:
    for element in result:
        if element in ['LA', 'LL', 'BT']:
            queryD = "select t.user_name, i.user_is_long, i.user_hobby from jsol_usertop t, jsol_userinfo i where t.user_idx=%s and t.userinfo_idx = i.userinfo_idx"
            cur.execute(queryD, (user_id))
            details = cur.fetchall()
            name = details[0][0]

            if element == 'LA':
                arrival_df = df[df['ATTEND_STATUS'].isin(['LA', 'NA'])].copy()
                arrival_df.loc[:, 'time'] = pd.to_datetime(arrival_df['REG_DATE']).dt.time
                arrival_df.loc[:, 'timedelta'] = pd.to_datetime(arrival_df['REG_DATE']) - pd.to_datetime(arrival_df['REG_DATE']).dt.normalize()

                # 기본 통계
                total_days = len(arrival_df)
                average_timedelta = arrival_df['timedelta'].mean()
                average_time = (datetime.min + average_timedelta).time()

                # 지각 분석
                late_arrivals = arrival_df[arrival_df['time'] >= dt_time(9,0)]
                late_rate = len(late_arrivals) / total_days * 100

                # 출근 시간 분포 분석
                time_bins = [timedelta(hours=8, minutes=30), timedelta(hours=8, minutes=45), timedelta(hours=9), timedelta(hours=9, minutes=15), timedelta(hours=9, minutes=30), timedelta(hours=23, minutes=59)]
                labels = ['8:30-8:44', '8:45-8:59', '9:00-9:14', '9:15-9:29', '9:30 이후']
                arrival_df.loc[:, 'time_category'] = pd.cut(arrival_df['timedelta'], bins=time_bins, labels=labels, include_lowest=True)
                time_distribution = arrival_df['time_category'].value_counts().sort_index()

                # 요일별 분석
                arrival_df.loc[:, 'weekday'] = pd.to_datetime(arrival_df['REG_DATE']).dt.day_name()
                weekday_avg = arrival_df.groupby('weekday')['timedelta'].mean().apply(lambda x: (datetime.min + x).time())

                # 월별 분석
                arrival_df.loc[:, 'month'] = pd.to_datetime(arrival_df['REG_DATE']).dt.month
                monthly_avg = arrival_df.groupby('month')['timedelta'].mean().apply(lambda x: (datetime.min + x).time())

                #print(f"총 기록 일수: {total_days}일")
                #print(f"평균 출근 시간: {average_time.strftime('%H:%M')}")
                #print(f"지각률: {late_rate:.2f}%")
                #print(f"\n출근 시간 분포:\n{time_distribution}")
                #print(f"\n요일별 평균 출근 시간:\n{weekday_avg}")
                #print(f"\n월별 평균 출근 시간:\n{monthly_avg}")
                
                if details[0][1] == b'\x00':
                    type = "단거리 통근 지각빈발형"
                    prompt.append(f"너는 직원(이름: {name})의 출퇴근시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {total_days}일, 평균 출근 시간: {average_time.strftime('%H:%M')}, 지각 기준 시각을 9시 이후로 했을때 지각률: {late_rate:.2f}%, 출근 시간 분포:{time_distribution}, 요일별 평균 출근 시간:\n{weekday_avg}, 월별 평균 출근 시간:\n{monthly_avg}\n이야. (데이터에서 유의미한 결론을 도출해서 작성해줘.) 원인: 시간 관리 문제, 아침 루틴 부재 등등, 해결책: 시간 관리, 아침 루틴(규칙적인 수면시간, 식습관) 만들기 등 이고, 이를 활용하여 (출근시간 및 지각패턴 분석->원인 추측 및 건강 리스크->권장사항 및 해결책) 순서로 리포트를 줄글로 작성해줘.")
                if details[0][1] == b'\x01':
                    type = "장거리 통근 지각빈발형"
                    hobby = details[0][2]
                    prompt.append(f"너는 직원(이름: {name})의 출퇴근시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {total_days}일, 평균 출근 시간: {average_time.strftime('%H:%M')}, 지각 기준 시각을 9시 이후로 했을때 지각률: {late_rate:.2f}%, 출근 시간 분포:{time_distribution}, 요일별 평균 출근 시간:\n{weekday_avg}, 월별 평균 출근 시간:\n{monthly_avg}\n이야. (데이터에서 유의미한 결론을 도출해서 작성해줘.) 건강 리스크: 만성 피로, 좌식생활 건강문제(허리..), 스트레스, 우울증 등등, 해결책: 수면시간 확보, 건강 관리 및 우울증 예방를 위한 균형잡힌 식사와 규칙적인 운동이나 취미활동({hobby}), 근무시간 조절 권장 등 이고, 이를 활용하여 (출근시간 및 지각패턴 분석->원인 추측 및 건강 리스크->권장사항 및 해결책) 순서로 리포트를 줄글로 작성해줘.")

            elif element == 'LL':
                overtime_df = df[df['ATTEND_STATUS'] == 'LL'].copy()
                overtime_df.loc[:, 'time'] = pd.to_datetime(overtime_df['REG_DATE']).dt.time
                overtime_df.loc[:, 'timedelta'] = pd.to_datetime(overtime_df['REG_DATE']) - pd.to_datetime(overtime_df['REG_DATE']).dt.normalize()

                # 정규 퇴근 시간
                regular_end_time = dt_time(18, 0)

                # 야근 시간 계산
                overtime_df.loc[:, 'overtime'] = overtime_df['time'].apply(lambda x: 
                    timedelta(hours=x.hour - regular_end_time.hour, 
                            minutes=x.minute - regular_end_time.minute, 
                            seconds=x.second - regular_end_time.second) 
                    if x > regular_end_time else timedelta(0))

                # 평균 야근 시간 계산
                average_overtime = overtime_df['overtime'].mean()

                # 평균 야근 시간을 시:분:초 형식으로 변환
                average_overtime_str = str(average_overtime)
                if 'days' in average_overtime_str:
                    days, time = average_overtime_str.split(' days ')
                    hours, minutes, seconds = time.split(':')
                    total_hours = int(days) * 24 + int(hours)
                    average_overtime_str = f"{total_hours:02d}:{minutes}:{seconds.split('.')[0]}"
                else:
                    average_overtime_str = average_overtime_str.split('.')[0]

                overtime_days = len(overtime_df)
                overtime_rate = (overtime_days / normalAL) * 100

                #print(f"총 근무 일수: {normalAL}일")
                #print(f"야근 일수: {overtime_days}일")
                #print(f"야근 비율: {overtime_rate:.2f}%")
                #print(f"평균 야근 시간: {average_overtime_str}")

                type = "야근빈발형"
                prompt.append(f"너는 직원(이름: {name})의 근무시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {normalAL}일, 야근 일수: {overtime_days}일, 야근 비율: {overtime_rate:.2f}%, 평균 초과 근무 시간: {average_overtime_str}이야. (야근 일수 통계 분석->건강 리스크->권장사항) 순서로 리포트를 줄글로 작성해줘. (취미활동: {hobby})")
            
            elif element == 'BT':
                business_trip_df = df[df['ATTEND_STATUS'] == 'BT'].copy()

                total_days = normalAL + dict['BT']  #총 출근일 수
                business_days = len(business_trip_df)

                #print(f"총 근무 일수: {total_days}일")
                #print(f"출장 일수: {business_days}일")

                type = "출장빈발형"
                prompt.append(f"너는 직원(이름: {name})의 근무 환경을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 근무 일수: {total_days}일, 출장 일수: {business_days}일이야. (출장 일수 통계->건강 리스크->권장사항) 순서로 리포트를 줄글로 작성해줘.")
    if len(prompt) == 0:
        print("OT, EL 표시")
    elif len(prompt) == 1:
        response = model.generate_content('\n'.join(prompt))
        print(response.text)
    elif len(prompt) >= 2:
        response = model.generate_content('해당하는 유형들에 대한 복합적인 리포트를 정리해서 작성해줘.' + '\n'.join(prompt))
        print(response.text)
                
#db 업데이트 (리포트 테이블에 유형+리포트)

cur.close()
conn.close()