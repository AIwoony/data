import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

def authenticate_gspread():
    print("Starting Google Sheets API authentication")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('C:/sangwoon/backend/credentials.json', scope)
    client = gspread.authorize(creds)
    print("Authentication successful")
    return client

def get_spreadsheet(client, name):
    try:
        spreadsheet = client.open(name)
        return spreadsheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet named '{name}' not found. Please check the name and try again.")
        raise

def main():
    client = authenticate_gspread()
    spreadsheet_name = "[WFA] 거점 BI REPORT"
    spreadsheet = get_spreadsheet(client, spreadsheet_name)

    # raw 데이터를 읽기
    raw_data_sheet = spreadsheet.worksheet("거점오피스 이용량")
    data = raw_data_sheet.get_all_records()
    df = pd.DataFrame(data)

    # 데이터 변환: 유니크한 사용자를 기반으로 하루에 지점별 방문 수 계산
    df_unique = df.drop_duplicates(subset=['enteredDate', 'mocaUserKey', 'device_group_name2', 'corpName'])
    result = df_unique.groupby(['device_group_name2', 'enteredDate', 'corpName']).size().reset_index(name='방문수')

    # 'rawdata(2)' 시트가 존재하는지 확인하고, 없으면 생성
    try:
        new_sheet = spreadsheet.worksheet("rawdata(2)")
        new_sheet.clear()  # 기존 데이터 지우기
    except gspread.exceptions.WorksheetNotFound:
        new_sheet = spreadsheet.add_worksheet(title="rawdata(2)", rows="1000", cols="20")

    # 컬럼 헤더 추가
    header = ['지점', '출입날짜', '회사 이름', '방문수']
    new_sheet.append_row(header)

    # 데이터를 일괄 추가
    rows = result.values.tolist()
    new_sheet.append_rows(rows, table_range="A2")

    print("Data transformation and saving completed")

if __name__ == "__main__":
    main()
