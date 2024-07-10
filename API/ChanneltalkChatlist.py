import requests
import pandas as pd

# 채널톡 Access Key와 Access Secret
access_key = '667901cbdba5f610e779'
access_secret = 'f1bad05007fe44c4fbb43192a9238291'

# 요청 URL
url = 'https://api.channel.io/open/v5/user-chats'

# 헤더 설정
headers = {
    'x-access-key': access_key,
    'x-access-secret': access_secret,
    'Content-Type': 'application/json'
}

# 쿼리 파라미터 설정
params = {
    'state': 'opened',  # 조회할 채팅 상태
    'sortOrder': 'desc',  # 정렬 순서
    'limit': 40  # 한 번에 가져올 최대 채팅 수
}

# GET 요청 보내기
response = requests.get(url, headers=headers, params=params)

# 요청이 성공했는지 확인
if response.status_code == 200:
    data = response.json()
    user_chats = data.get('userChats', [])
    
    # CSV 파일로 저장
    df = pd.DataFrame(user_chats)
    df.to_csv('user_chats.csv', index=False)
    print('CSV 파일로 저장되었습니다.')
else:
    print(f'유저 채팅을 가져오는 데 실패했습니다. 상태 코드: {response.status_code}, 응답: {response.text}')
