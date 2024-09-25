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

    root: str = str(Path(__file__).parent.parent.parent)  # apt_trade
    src: str = str(Path(root).joinpath("src"))  # apt_trade/src
    data: str = str(Path(src).joinpath("data"))  # apt_trade/src/data
    snapshot: str = str(
        Path(data).joinpath("snapshots")
    )  # apt_trade/src/data/snpashots
    history: str = str(Path(data).joinpath("history"))  # apt_trade/src/data/history
    metastore: str = str(Path(src).joinpath("metastore"))  # apt_trade/src/metastore


class URLDictionary:
    URL: dict = {
        "apt_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "lawd_cd": "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList",
        "bunyang_trade": "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
    }
    FakeAgent: str = "u'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36'"


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
        "slerGbn": "매수자",
        "buyerGbn": "매도자",
        "estateAgentSggNm": "중개사소재지",
        "sggCd": "시군구코드",
        "umdNm": "법정동",
        "tradeGbn": "거래구분",
    }


class FilterDictionary:
    sgg_contains: list = [
        "서초구",
        "강남구",
        "송파구",
        "마포구",
        "용산구",
        "성동구",
        "강동구",
    ]

    apt_contains: list = [
        "헬리오시티",
        "마포래미안푸르지오",
        "마포프레스티지자이",
        "더클래시",
        "올림픽파크포레온",
        "잠실엘스",
        "리센츠",
        "파크리오",
        "고덕그라시움",
        "고덕아르테온",
        "옥수하이츠",
    ]

    apt_code: dict = {
        "헬리오시티": "111515",
        "마포래미안푸르지오": "104917",
        "마포프레스티지자이": "121608",
        "더클래시": "148651",
        "올림픽파크포레온": "155817",
        "잠실엘스": "22627",
        "리센츠": "22746",
        "파크리오": "22675",
        "고덕그라시움": "113907",
        "고덕아르테온": "119341",
        "옥수하이츠": "564",
    }
