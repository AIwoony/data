import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime

# Google Sheets API 인증
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/sangwoon/backend/credentials.json', scope)
client = gspread.authorize(creds)

# Google Sheets 문서와 시트 접근
spreadsheet = client.open("[WFA] 거점 BI REPORT")  # 스프레드시트 이름g
source_sheet = spreadsheet.worksheet("거점오피스 이용량")
target_sheet_name = '회사별 일자별 데이터'

# 시트에서 데이터 가져오기
data = source_sheet.get_all_values()
header = data.pop(0)

# 데이터 프레임으로 변환
df = pd.DataFrame(data, columns=header)

# 데이터 프레임의 열 이름 출력
print("Column names:", df.columns)

# 데이터 프레임의 첫 몇 행 출력
print("First few rows of data:", df.head())

# 날짜 형식 변환 및 필터링
df['enteredDate'] = pd.to_datetime(df['enteredDate'], errors='coerce')
df = df.dropna(subset=['enteredDate'])

# 회사별 일자별 데이터 처리
company_data = {}
dates_set = set()
months_set = set()

for _, row in df.iterrows():
    date = row['enteredDate']
    moca_user_key = row['mocaUserKey']
    corp_name = row['corpName']

    date_str = date.strftime('%Y-%m-%d')
    month_str = date.strftime('%Y-%m')

    if corp_name not in company_data:
        company_data[corp_name] = {'daily': {}, 'monthly': {}}

    if date_str not in company_data[corp_name]['daily']:
        company_data[corp_name]['daily'][date_str] = set()
    if month_str not in company_data[corp_name]['monthly']:
        company_data[corp_name]['monthly'][month_str] = set()

    company_data[corp_name]['daily'][date_str].add(moca_user_key)
    company_data[corp_name]['monthly'][month_str].add(moca_user_key)
    dates_set.add(date_str)
    months_set.add(month_str)

dates = sorted(dates_set, key=lambda x: datetime.strptime(x, '%Y-%m-%d'), reverse=True)
months = sorted(months_set, key=lambda x: datetime.strptime(x, '%Y-%m'), reverse=True)

# 헤더 생성
header_row = ['회사 이름']
for month in months:
    header_row.append(f'{month} 합계')
    for date in dates:
        if date.startswith(month):
            header_row.append(date)

# 데이터 프레임 생성
output_data = []
date_sums = [0] * len(header_row)
date_sums[0] = '일자별 합계'

for corp_name, data in company_data.items():
    row = [corp_name]
    col_idx = 1
    for month in months:
        month_sum = 0
        month_col_idx = col_idx
        row.append(0)
        col_idx += 1
        for date in dates:
            if date.startswith(month):
                daily_count = len(data['daily'].get(date, set()))
                row.append(daily_count)
                month_sum += daily_count
                date_sums[col_idx] += daily_count
                col_idx += 1
        row[month_col_idx] = month_sum
        date_sums[month_col_idx] += month_sum
    output_data.append(row)

# 데이터 프레임 생성 및 정렬
output_df = pd.DataFrame(output_data, columns=header_row)
output_df.sort_values(by=[f'2024-07 합계'], ascending=False, inplace=True)

# Google Sheets에 업로드
if target_sheet_name in [sheet.title for sheet in spreadsheet.worksheets()]:
    spreadsheet.del_worksheet(spreadsheet.worksheet(target_sheet_name))
target_sheet = spreadsheet.add_worksheet(title=target_sheet_name, rows="1000", cols="100")

# 헤더 업로드
target_sheet.append_row(header_row)

# 데이터 업로드
target_sheet.append_row(date_sums)
for row in output_df.values.tolist():
    target_sheet.append_row(row)
