from encryption_util import EncryptionUtil
from fastapi import FastAPI, BackgroundTasks
import pymysql
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import pymysql
import google.generativeai as genai
from kiwipiepy import Kiwi
from collections import Counter

import os
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

kiwi = Kiwi()

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key = GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

def get_db_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        port=3308,
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8mb4'
    )

def calculate_age(birth_date):
    today = datetime.now()
    birth_year = int(birth_date[:2])
    birth_month = int(birth_date[2:4])
    birth_day = int(birth_date[4:6])
    
    # 2000년 이후 출생자 구분
    if birth_year > 24:
        birth_year += 1900
    else:
        birth_year += 2000
    
    age = today.year - birth_year
    
    # 생일이 지났는지
    if (today.month, today.day) < (birth_month, birth_day):
        age -= 1
    
    return age

def defineSex(reg_num_lat):
    if int(str(reg_num_lat)[0]) % 2 == 0:
        sex = "여성"
    else:
        sex = "남성"

    return sex 

def analyzeType(statusCounts):
    late_arrive = 0.15
    late_leave = 0.28
    early_leave = 0.08
    out = 0.15
    business_trip = 10

    result = []

    normalAL = statusCounts['NA'] + statusCounts['LA'] #dict['NL'] + dict['LL'] + dict['EL']와 같아야함

    if statusCounts['LA'] >= normalAL * late_arrive:
        result.append('LA')

    if statusCounts['LL'] >= normalAL * late_leave:
        result.append('LL')

    if statusCounts['BT'] >= business_trip:
        result.append('BT')

    if statusCounts['EL'] >= normalAL * early_leave:
        result.append('EL')

    if statusCounts['OT'] >= normalAL * out:
        result.append('OT')

    return normalAL, result

def getKeywords(report):

    tokens = kiwi.analyze(report)

    nouns = []
    tokens = kiwi.analyze(report)
    for token, pos, _, _ in tokens[0][0]:
        if len(token) != 1 and pos in ['NNG', 'NNP', 'NR', 'XR', 'SL', 'SH', 'SN']:
            nouns.append(token)

    noun_counts = Counter(nouns)
    sorted_nouns = noun_counts.most_common(8)

    keyword_list = []
    for noun in sorted_nouns:
        keyword_list.append(noun[0])

    return keyword_list


def generate_and_save_reports():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        query1 = "SELECT DISTINCT ADMIN_IDX FROM jsol_attendance_record"
        cur.execute(query1)
        admin_ids = cur.fetchall()
        
        query2 = "SELECT DISTINCT USER_IDX FROM jsol_attendance_record"
        cur.execute(query2)
        user_ids = cur.fetchall()
        
        ids = {'ADMIN_IDX':admin_ids, 'USER_IDX':user_ids}
        for key in ids:
            for i in range(len(ids[key])):
                id = ids[key][i][0]
                if id != None:
                    #print(key, id)
                    query3 = f"SELECT {key}, ATTEND_STATUS, REG_DATE FROM jsol_attendance_record WHERE {key} = {id}"
                    cur.execute(query3)
                    data = cur.fetchall()
                    df = pd.DataFrame(data, columns=[key, 'ATTEND_STATUS', 'REG_DATE'])
                    statusCount = df['ATTEND_STATUS'].value_counts().to_dict()
                    normalAL, result = analyzeType(statusCount)
                    
                    prompt = []
                    if result:
                        for element in result:
                            if element in ['LA', 'LL', 'BT']:
                                if key=="ADMIN_IDX":
                                    query4 = f"select t.admin_name, t.reg_number_for, t.reg_number_lat, i.user_is_long, i.user_hobby from jsol_admintop t, jsol_userinfo i where t.{key}={id} and t.userinfo_idx = i.userinfo_idx"
                                elif key=="USER_IDX":
                                    query4 = f"select t.user_name, t.reg_number_for, t.reg_number_lat, i.user_is_long, i.user_hobby from jsol_usertop t, jsol_userinfo i where t.{key}={id} and t.userinfo_idx = i.userinfo_idx"
                                cur.execute(query4)
                                details = cur.fetchall()
                                name = details[0][0]
                                regNumFor = details[0][1]
                                encryption_util = EncryptionUtil()
                                age = calculate_age(encryption_util.decrypt(regNumFor))
                                regNumLat = details[0][2]
                                sex = defineSex(encryption_util.decrypt(regNumLat))
                                isLong = details[0][3]
                                hobby = details[0][4]

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
                                    
                                    if isLong == b'\x00':
                                        type = "단거리 통근 지각빈발형"
                                        prompt.append(f"너는 직원(이름: {name})의 출퇴근시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {total_days}일, 평균 출근 시간: {average_time.strftime('%H:%M')}, 지각 기준 시각을 9시 이후로 했을때 지각률: {late_rate:.2f}%, 출근 시간 분포:{time_distribution}, 요일별 평균 출근 시간:\n{weekday_avg}, 월별 평균 출근 시간:\n{monthly_avg}\n이야. (데이터에서 유의미한 결론을 도출해서 작성해줘.) 원인: 시간 관리 문제, 아침 루틴 부재 등등, 해결책: 시간 관리, 아침 루틴(규칙적인 수면시간, 식습관) 만들기 등 이고, 이를 활용하여 (출근시간 및 지각패턴 분석->원인 추측 및 건강 리스크->권장사항 및 해결책) 순서로 리포트를 줄글로 작성해줘. 이 직원은 {sex}이며, 만 {age}세야.")
                                    if isLong == b'\x01':
                                        type = "장거리 통근 지각빈발형"
                                        prompt.append(f"너는 직원(이름: {name})의 출퇴근시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {total_days}일, 평균 출근 시간: {average_time.strftime('%H:%M')}, 지각 기준 시각을 9시 이후로 했을때 지각률: {late_rate:.2f}%, 출근 시간 분포:{time_distribution}, 요일별 평균 출근 시간:\n{weekday_avg}, 월별 평균 출근 시간:\n{monthly_avg}\n이야. (데이터에서 유의미한 결론을 도출해서 작성해줘.) 건강 리스크: 만성 피로, 좌식생활 건강문제(허리..), 스트레스, 우울증 등등, 해결책: 수면시간 확보, 건강 관리 및 우울증 예방를 위한 균형잡힌 식사와 규칙적인 운동이나 취미활동({hobby}), 근무시간 조절 권장 등 이고, 이를 활용하여 (출근시간 및 지각패턴 분석->원인 추측 및 건강 리스크->권장사항 및 해결책) 순서로 리포트를 줄글로 작성해줘. 이 직원은 {sex}이며, 만 {age}세야.")

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
                                    prompt.append(f"너는 직원(이름: {name})의 근무시간을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 기록 일수: {normalAL}일, 야근 일수: {overtime_days}일, 야근 비율: {overtime_rate:.2f}%, 평균 초과 근무 시간: {average_overtime_str}이야. (야근 일수 통계 분석->건강 리스크->권장사항) 순서로 리포트를 줄글로 작성해줘. (취미활동: {hobby}, 성별: {sex}, 나이: 만 {age}세)")
                                
                                elif element == 'BT':
                                    business_trip_df = df[df['ATTEND_STATUS'] == 'BT'].copy()

                                    total_days = normalAL + dict['BT']  #총 출근일 수
                                    business_days = len(business_trip_df)

                                    #print(f"총 근무 일수: {total_days}일")
                                    #print(f"출장 일수: {business_days}일")

                                    type = "출장빈발형"
                                    prompt.append(f"너는 직원(이름: {name})의 근무 환경을 통해 웰니스 리포트를 작성하는 주체야. 이 직원은 {type}이고, 총 근무 일수: {total_days}일, 출장 일수: {business_days}일이야. (출장 일수 통계->건강 리스크->권장사항) 순서로 리포트를 줄글로 작성해줘. 이 직원은 {sex}이며, 만 {age}세야.")
                        
                        current_datetime = datetime.now().isoformat()
                        if len(prompt) == 0:
                            cur.execute(f"INSERT INTO jsol_attendance_report (REPORTED_IDX, REPORTED_IS_ADMIN, REPORT_USER_TYPE, REPORTER_IDX, IS_SENT, REG_DATE) VALUES ({id}, {1 if key == "ADMIN_IDX" else 0}, '{','.join(result)}', 17, 0, '{current_datetime}')")
                        elif len(prompt) == 1:
                            response = model.generate_content('\n'.join(prompt))
                            #print(response.text)
                            report = response.text
                            keywords= ','.join(getKeywords(report))
                            #print(keywords)
                            cur.execute(f"INSERT INTO jsol_attendance_report (REPORTED_IDX, REPORTED_IS_ADMIN, REPORT_USER_TYPE, AI_REPORT_COMMENT, REPORTER_IDX, IS_SENT, REG_DATE, KEYWORDS) VALUES ({id}, {1 if key == "ADMIN_IDX" else 0}, '{','.join(result)}', '{report}', 17, 0, '{current_datetime}', '{keywords}')")
                        elif len(prompt) >= 2:
                            response = model.generate_content('해당하는 유형들에 대한 복합적인 리포트를 정리해서 작성해줘.' + '\n'.join(prompt))
                            #print(response.text)
                            report = response.text
                            keywords= ','.join(getKeywords(report))
                            #print(keywords)
                            cur.execute(f"INSERT INTO jsol_attendance_report (REPORTED_IDX, REPORTED_IS_ADMIN, REPORT_USER_TYPE, AI_REPORT_COMMENT, REPORTER_IDX, IS_SENT, REG_DATE, KEYWORDS) VALUES ({id}, {1 if key == "ADMIN_IDX" else 0}, '{','.join(result)}', '{report}', 17, 0, '{current_datetime}', '{keywords}')")
                        
                        conn.commit()
                        print(f"report insert 완료 ({key}: {id}, type: {result})")

    except Exception as e:
        print(f"Error from generating report: {e}")
    finally:
        cur.close()
        conn.close()

@app.post("/generate-reports/")
async def generate_report(background_tasks: BackgroundTasks):
    background_tasks.add_task(generate_and_save_reports)
    return {"message": "리포트 생성이 백그라운드에서 시작되었습니다."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)