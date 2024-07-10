companies = [
    ("아식스", "asics"),
    ("포티넷 코리아", "fortinet"),
    ("한일홀딩스", "hanil"),
    ("현대자동차 디자인센터", "hydc"),
    ("한국정보통신진흥협회", "kait"),
    ("카카오브레인", "kakaobrain"),
    ("KT", "kt"),
    ("루트로닉", "lutronic"),
    ("미래에셋증권", "miraeasset"),
    ("보임테크놀러지", "voimtech"),
    ("동아쏘시오홀딩스", "dongasocio"),
    ("크림 주식회사", "kreamcorp"),
    ("토스뱅크", "tossbank"),
    ("녹십자", "gccorp"),
    ("삼성증권", "samsungpop"),
    ("나이스정보통신(주)", "nicevan"),
    ("GSK", "gsk"),
    ("콜렉트웍스", "collectworks"),
    ("동아제약", "dapharm"),
    ("에스에스지닷컴", "ssg"),
    ("두산그룹", "doosan"),
    ("두산인베스트먼트", "doosaninvestment"),
    ("두산퓨얼셀", "doosanfuelcell"),
    ("두산경영연구원", "doosandbri"),
    ("삼성", "samsung"),
    ("나이스페이먼츠(주)", "nicepay"),
    ("(주)와이어트", "wyattcorp"),
    ("엘엑스판토스", "lxpantos"),
    ("CJ올리브네트웍스", "cj"),
    ("포스코건설", "poscoenc"),
    ("현대무벡스 주식회사", "hyundaimovex"),
    ("야놀자", "yanolja"),
    ("매일유업 라운지", "maeillounge"),
    ("헬로우워크 천안불당점", "helloworkcheonan"),
    ("머니투데이", "mt"),
    ("한국석유공업(주)", "koreapetroleum"),
    ("마이크로웨이브 대구점", "microwavedaegu"),
    ("두산로보틱스", "doosanrobotics"),
    ("두산VCC", "doosanvcc"),
    ("제일기획", "cheil"),
    ("노마드워크", "nomadwork"),
    ("LG에너지솔루션", "lgensol"),
    ("주식회사 이롭게", "iropke"),
    ("와디즈", "wadiz"),
    ("가온전선 (주)", "gaoncable"),
    ("롯데칠성", "lotte"),
    ("티맥스소프트", "tmaxsoft"),
    ("삼일회계법인", "pwc"),
    ("롯데웰푸드", "lottewellfood"),
    ("주식회사 체커", "chequer"),
    ("아일라애니웨어", "ayla"),
    ("DA인포메이션", "dainform"),
    ("동아ST", "dongast"),
    ("카카오엔터프라이즈", "kakaoenterprise"),
    ("카카오", "kakao"),
    ("KB부동산신탁", "kbret"),
    ("한국공예디자인문화진흥원", "kcdf"),
    ("KBS", "kbs"),
    ("포스코에너지", "poscoenergy"),
    ("주식회사 이오스튜디오", "eoeoeo"),
    ("현대자동차(주) 남양연구소", "hyundai"),
    ("커먼타운", "commontown"),
    ("디엘건설 주식회사", "dlcon"),
    ("버닝갤럭시코리아(유)", "burninggalaxy"),
    ("주식회사 그린랩스", "greenlabs"),
    ("KIS정보통신(주)", "kisvan"),
    ("토스페이먼츠", "tosspayments"),
    ("엔디에스", "nds"),
    ("동아쏘시오그룹", "donga"),
    ("현대제철", "hyundaisteel"),
    ("서울경제", "sedaily"),
    ("한국개발연구원", "kdi"),
    ("두산디지털이노베이션", "doosanddi"),
    ("신세계아이앤씨", "shinsegae"),
    ("LGDisplay", "lgd"),
    ("두산매거진", "doosanmagazine"),
    ("주식회사 비즈테크아이", "biztechi"),
    ("매일유업", "maeil"),
    ("LG CNS", "lgcns"),
    ("ASML(에이에스엠엘)", "asml"),
    ("레몬베이스", "lemonbase"),
    ("에스제이에이치스튜디오", "sjhstudio"),
    ("알비언 주식회사", "arbeon"),
    ("주식회사 스토리73", "story"),
    ("아주약품", "ajupharm"),
    ("쏘카", "socar"),
    ("엘지디스플레이", "lgdisplay"),
    ("비전포트", "visionport"),
    ("동아오츠카", "dongaotsuka"),
    ("삼정회계법인", "krkpmg"),
    ("두산지주부문", "doosancorp"),
    ("더워크", "thework"),
    ("마이크로웨이브", "microwave")
]

query_template = """

select  '{corp_name}' as corp_name, mu.email, count(Distinct marv.entered_date), min(marv.entered_date)
    from {db_prefix}.members_user mu
    left join {db_prefix}.moca_access_record_view marv
    on marv.email = mu.email
    group by mu.email

    
"""

queries = []

for corp_name, db_prefix in companies:
    queries.append(query_template.format(corp_name=corp_name, db_prefix=db_prefix))

final_query = " UNION ALL ".join(queries)

full_query = f"SELECT * FROM (\n{final_query}\n) AS combined_reservations limit 30000"

print(full_query)

# 텍스트 파일로 내보내기
with open("final_query.txt", "w", encoding="utf-8") as file:
    file.write(full_query)

print("SQL 쿼리가 final_query.txt 파일로 저장되었습니다.")
