import asyncio
import telegram
from textwrap import dedent

from .utils import load_env

class TelegramTemplate:

    DAILY_TRADE = dedent("""
    ⭐ {{ date_id }}일 기준 {{ month }}월 서울 전체 실거래 {{ total_trade }}건
    * 시군구별 실거래
    {%- for sgg_nm, trades, cancels in zip(sgg_list, apt_trade, apt_trade_cancels) %}
    - {{ sgg_nm }}: 실거래({{ trades }}) | 계약해지({{ cancels }})
    {%- endfor %}""")