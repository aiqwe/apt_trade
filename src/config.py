class Dictionary:
    BASE_URL = {
        "apt_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "lawd_cd": "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList",
        "bunyang_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
    }

    LAWD_CD_DICTIONARY = {
        "region_cd": "지역코드",
        "sido_cd": "시도코드",
        "sgg_cd": "시군구코드",
        "umd_cd": "읍면동코드",
        "locatadd_nm": "지역명",
        "locathigh_cd": "상위코드",
    }

    APT_TRADE_DICTIONARY = {
        "aptNm": "아파트명",
        "excluUseAr": "전용면적",
        "dealYear": "계약년도",
        "dealMonth": "계약월",
        "dealDay": "계약일",
        "dealAmount": "거래금액",
        "floor": "층",
        "buildYear": "건축년도",
        # "ownershipGbn": "권리구분",
        "cdealType": "계약해지여부",
        "cdealDay": "계약해지사유발생일",
        "dealingGbn": "거래유형",
        "rgstDate": "등기일자",
        "aptDong": "동",
        "slerGbn": "매수자",
        "buyerGbn": "매도자",
        "estateAgentSggNm": "중개사소재지",
        "sggCd": "시군구코드",
        "umdNm": "법정동"
    }

    BUNYANG_TRADE_DICTIONARY = {
        "aptNm": "아파트명",
        "excluUseAr": "전용면적",
        "dealYear": "계약년도",
        "dealMonth": "계약월",
        "dealDay": "계약일",
        "dealAmount": "거래금액",
        "floor": "층",
        # "buildYear": "건축년도",
        "ownershipGbn": "권리구분",
        "cdealType": "계약해지여부",
        "cdealDay": "계약해지사유발생일",
        "dealingGbn": "거래유형",
        # "rgstDate": "등기일자",
        "aptDong": "동",
        "slerGbn": "매수자",
        "buyerGbn": "매도자",
        "estateAgentSggNm": "중개사소재지",
        "sggCd": "시군구코드",
        "umdNm": "법정동"
    }

def convert_column(dictionary: dict, df: 'pd.DataFrame' = None, column_name: str = None, alive=True):
    """ 컬럼이름 src -> dst로 변환, column_name이 df보다 우선하여 처리된다

    Args:
        dictionary: 변환을 사용할 dictionary 테이블
        df: 변환할 데이터프레임
        column_name: 변환할 컬럼명
        alive: 변환이 된 컬럼만 남긴다
    """
    if df is None and not column_name:
        raise ValueError("you should assign one of 'df' or 'column_name'")

    if column_name:
        return dictionary[column_name]
    if df is not None:
        columns = []
        alive = []
        for col in df.columns:
            if col in dictionary:
                columns.append(dictionary[col])
                alive.append(dictionary[col])
            else:
                columns.append(col)
        df.columns = columns
        if alive:
            df = df[alive]
        return df