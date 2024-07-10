import os
import pandas as pd

# 필터링된 파일들이 있는 폴더 경로 (raw 문자열 사용)
filtered_folder = os.path.join(os.path.expanduser('~'), 'Desktop', '필터링된파일')

# 바탕화면에 새 폴더 생성 (결과 파일을 저장할 폴더)
output_folder = os.path.join(os.path.expanduser('~'), 'Desktop', '결과파일')
os.makedirs(output_folder, exist_ok=True)

# 결과를 저장할 빈 데이터프레임 생성
result_df = pd.DataFrame()

# 폴더 내의 모든 필터링된 CSV 파일 순회
for file_name in os.listdir(filtered_folder):
    if file_name.startswith('filtered_exportEventLogs') and file_name.endswith('.csv'):
        try:
            # 파일 경로
            file_path = os.path.join(filtered_folder, file_name)
            print(f"Combining file: {file_path}")

            # CSV 파일 읽기 (인코딩 지정)
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            print(f"Read {len(df)} rows from {file_name}")

            # 결과 데이터프레임에 추가
            result_df = pd.concat([result_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

# 결과 데이터프레임을 하나의 CSV 파일로 저장 (인코딩 지정)
if not result_df.empty:
    result_file_path = os.path.join(output_folder, 'combined_filtered_results.csv')
    result_df.to_csv(result_file_path, index=False, encoding='utf-8-sig')
    print(f"Saved combined filtered results to {result_file_path}")
else:
    print("No data found to combine.")

print("Combining completed.")
