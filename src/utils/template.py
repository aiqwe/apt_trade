from textwrap import dedent


class TelegramTemplate:
    DAILY_STATUS = dedent("""
    ⭐ {{ date_id }}일 기준 실거래 월별 집계
    * {{ month }}월 서울 전체 실거래 {{ total_trade }}건
    * 전일 대비 +{{ change }} 건
    * 시군구별 실거래
    {%- for sgg_nm, trades, cancels, news in zip(sgg_list, apt_trades, apt_trade_cancels, new_trades) %}
    - {{ sgg_nm }}: 계약({{ trades }}) | 해지({{ cancels }}) | 전일대비(+{{ news }})
    {%- endfor %}"""
    )

    DAILY_DIFFERENCE = dedent("""
    ⭐ {{ date_id }}일 기준 실거래 상세 현황
    * {{ month }}월 거래분
    {%- if len(data) == 0 %}
    없음{% else %}
    {%- for row in data %}
    {%- for k, v in row.items() %}
    {% if loop.first %} 🏠 [{{ v }}]
    {%- else %}  - {{ k }}: {{ v }}
    {%- endif %}
    {%- endfor %}
    
    {% endfor %}
    {% endif %}
    """)