from slack_sdk.webhook import WebhookClient
from .config import URLDictionary
from .utils import load_env
from loguru import logger
from jinja2 import Template

def send_message(url: str = None, text: str = None, blocks: list = None):
    if not url:
        url = load_env("SLACK_WEB_HOOK_URL", ".env")
    webhook = WebhookClient(url=url)

    response = webhook.send(
        text = text,
        blocks = blocks
    )

    if any([response.status_code != 200, response.body != "ok"]):
        logger.error(f"{response.status_code=}\n{response.body=}")

class BlockTemplate:

    @staticmethod
    def render(template, **params):
        rendered = Template(template).render(**params, zip=zip, len=len)
        return rendered

    DAILY_MESSAGE = """
    {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":star: {{ date_id }}일 기준 {{ month }}월 서울 전체 실거래 {{ total_trade }}건"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "시군구별 실거래\\n"
                            }
                        ]
                    },
                    {
                        "type": "rich_text_list",
                        "style": "bullet",
                        "elements": [
                        {% for sgg_nm, trades, cancels in zip(sgg_list, apt_trade, apt_trade_cancels) %}
                            {
                                "type": "rich_text_section",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": "{{ sgg_nm }}: 실거래({{ trades }}) | 계약해지({{ cancels }})"
                                    }
                                ]
                            }{% if not loop.last %},{% else %}{% endif %}
                            {%- endfor %}
                        ]
                    }
                ]
            }
        ]
    }
    """
