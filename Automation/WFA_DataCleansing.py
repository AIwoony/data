import os
import pandas as pd

# 원본 파일들이 있는 폴더 경로 (raw 문자열 사용)
input_folder = r'C:\Users\SWHAN563-FASTFIVE\Desktop\입출입기록'

# 바탕화면에 새 폴더 생성 (필터링된 파일을 저장할 폴더)
filtered_folder = os.path.join(os.path.expanduser('~'), 'Desktop', '필터링된파일')
os.makedirs(filtered_folder, exist_ok=True)

# 폴더 내의 모든 CSV 파일 순회
for file_name in os.listdir(input_folder):
    if file_name.startswith('exportEventLogs') and file_name.endswith('.csv'):
        try:
            # 파일 경로
            file_path = os.path.join(input_folder, file_name)
            print(f"Processing file: {file_path}")

            # CSV 파일 읽기 (인코딩 지정)
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            print(f"Read {len(df)} rows from {file_name}")
            print(f"Columns in the file: {df.columns.tolist()}")

            # '사용자 이메일' 열에서 '.gen'으로 끝나는 행 제외
            if '사용자 이메일' in df.columns and '장치 그룹' in df.columns:
                print("Columns '사용자 이메일' and '장치 그룹' found. Filtering rows.")
                filtered_df = df[~df['사용자 이메일'].astype(str).str.endswith('.gen', na=False)]
                print(f"Filtered down to {len(filtered_df)} rows after excluding '.gen' emails for {file_name}")

                # '장치 그룹' 열에서 '복합기'가 포함된 행 제외
                filtered_df = filtered_df[~filtered_df['장치 그룹'].astype(str).str.contains('복합기', na=False)]
                print(f"Filtered down to {len(filtered_df)} rows after excluding '복합기' for {file_name}")

                # 필터링된 데이터프레임을 새 파일로 저장 (인코딩 지정)
                if not filtered_df.empty:
                    new_file_name = f"filtered_{file_name}"
                    new_file_path = os.path.join(filtered_folder, new_file_name)
                    filtered_df.to_csv(new_file_path, index=False, encoding='utf-8-sig')
                    print(f"Saved filtered file to {new_file_path}")
                else:
                    print(f"No rows meeting the criteria found in {file_name}")
            else:
                print(f"Required columns not found in {file_name}")
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

print("Processing completed.")
