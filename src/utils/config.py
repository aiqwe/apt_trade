from pathlib import Path

class PathDictionary:
    """
    Attributes:
        cls.root: apt_trade
        cls.src: apt_trade/src
        cls.data: apt_trade/src/data
        cls.snapshot: apt_trade/src/data/snapshots
        cls.history: apt_trade/src/data/history
    """
    root: str = str(Path(__file__).parent.parent.parent) # apt_trade
    src: str = str(Path(root).joinpath("src")) # apt_trade/src
    data: str = str(Path(src).joinpath("data")) # apt_trade/src/data
    snapshot: str = str(Path(data).joinpath("snapshots")) # apt_trade/src/data/snpashots
    history: str = str(Path(data).joinpath("history")) # apt_trade/src/data/history

class URLDictionary:
    URL = {
        "apt_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "lawd_cd": "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList",
        "bunyang_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
    }

class ColumnDictionary:
    LAWD_CD_DICTIONARY = {
        "region_cd": "지역코드",
        "sido_cd": "시도코드",
        "sgg_cd": "시군구코드",
        "umd_cd": "읍면동코드",
        "locatadd_nm": "지역명",
        "locathigh_cd": "상위코드",
    }
    # 실거래에 추가할 컬럼
    # "ownershipGbn": "권리구분",
    # "tradeGbn": "거래구분",
    # 분양권에 추가할 컬럼
    # "buildYear": "건축년도",
    # "rgstDate": "등기일자",
    # "tradeGbn": "거래구분",
    # "aptDong": "동"
    TRADE_DICTIONARY = {
        "aptNm": "아파트명",
        "buildYear": "건축년도",
        "excluUseAr": "전용면적",
        "dealAmount": "거래금액",
        "floor": "층",
        "aptDong": "동",
        "dealingGbn": "거래유형",
        "dealYear": "계약년도",
        "dealMonth": "계약월",
        "dealDay": "계약일",
        "cdealType": "계약해지여부",
        "cdealDay": "계약해지사유발생일",
        "rgstDate": "등기일자",
        "ownershipGbn": "권리구분",
        "rgstDate": "등기일자",
        "slerGbn": "매수자",
        "buyerGbn": "매도자",
        "estateAgentSggNm": "중개사소재지",
        "sggCd": "시군구코드",
        "umdNm": "법정동",
        "tradeGbn": "거래구분"
    }