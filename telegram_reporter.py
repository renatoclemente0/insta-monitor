from __future__ import annotations

import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Logger (consistente com classifier.py)
# ---------------------------------------------------------------------------

logger = logging.getLogger("telegram_reporter")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

TELEGRAM_MAX_MSG_LEN = 4096
MAX_QUOTE_LEN = 100
SEND_RETRIES = 2
RETRY_WAIT = [1.0, 2.0]
SEVERITY_THRESHOLD = 7.0
AMPLIFICATION_THRESHOLD = 7.5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_credentials() -> Optional[tuple]:
    """
    Retorna (token, chat_id) do Telegram a partir de variaveis de ambiente.
    Retorna None e loga erro se nao estiverem definidas.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.error(
            "MISSING_CREDENTIALS: TELEGRAM_BOT_TOKEN e/ou "
            "TELEGRAM_CHAT_ID nao definidos no .env"
        )
        return None
    return token, chat_id


def _truncate(text: Optional[str], max_len: int = MAX_QUOTE_LEN) -> str:
    """Trunca texto para max_len chars, adicionando '...' se necessario."""
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _escape_html(text: Optional[str]) -> str:
    """Escapa caracteres especiais para HTML do Telegram."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _send_telegram(token: str, chat_id: str, text: str) -> bool:
    """
    Envia uma mensagem via Telegram Bot API com retries.

    Trata erros 429 (rate limit) e timeouts de rede.
    Retorna True se enviou com sucesso, False caso contrario.
    """
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for attempt in range(SEND_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)

            if resp.status_code == 429:
                retry_after = 2.0
                try:
                    retry_after = float(
                        resp.json().get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    pass
                logger.warning(
                    "TELEGRAM_RATE_LIMIT: retry_after=%.1fs attempt=%d",
                    retry_after, attempt + 1,
                )
                if attempt < SEND_RETRIES:
                    time.sleep(retry_after)
                    continue
                return False

            resp.raise_for_status()
            return True

        except requests.exceptions.Timeout:
            logger.warning(
                "TELEGRAM_TIMEOUT: attempt=%d/%d",
                attempt + 1, SEND_RETRIES + 1,
            )
            if attempt < SEND_RETRIES:
                time.sleep(RETRY_WAIT[min(attempt, len(RETRY_WAIT) - 1)])
            else:
                logger.error("TELEGRAM_FAILED: todas tentativas esgotadas (timeout)")
                return False

        except requests.exceptions.RequestException as e:
            logger.error("TELEGRAM_ERROR: %s", e)
            return False

    return False


def _split_and_send(token: str, chat_id: str, text: str) -> bool:
    """
    Envia texto ao Telegram, dividindo em multiplas mensagens
    se exceder TELEGRAM_MAX_MSG_LEN (4096 chars).

    Divide por linhas para nao cortar tags HTML no meio.
    Retorna True se todas as partes foram enviadas com sucesso.
    """
    if len(text) <= TELEGRAM_MAX_MSG_LEN:
        return _send_telegram(token, chat_id, text)

    lines = text.split("\n")
    chunks = []  # type: List[str]
    current = ""

    for line in lines:
        candidate = current + line + "\n" if current else line + "\n"
        if len(candidate) > TELEGRAM_MAX_MSG_LEN:
            if current:
                chunks.append(current)
            current = line + "\n"
        else:
            current = candidate

    if current:
        chunks.append(current)

    all_ok = True
    for i, chunk in enumerate(chunks):
        ok = _send_telegram(token, chat_id, chunk.rstrip())
        if ok:
            logger.info(
                "TELEGRAM_SENT: parte %d/%d (%d chars)",
                i + 1, len(chunks), len(chunk),
            )
        else:
            all_ok = False
        if i < len(chunks) - 1:
            time.sleep(0.5)

    return all_ok


# ---------------------------------------------------------------------------
# Formatacao do relatorio
# ---------------------------------------------------------------------------


def _format_header(total: int) -> str:
    """Formata o cabecalho do relatorio com data/hora e total de videos."""
    now = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    return (
        "<b>RELATORIO DE MONITORAMENTO</b>\n"
        "<i>{now}</i> | <b>{total}</b> videos analisados\n"
    ).format(now=now, total=total)


def _format_ataques(ataques: List[Dict[str, Any]]) -> str:
    """
    Formata a secao de ataques criticos (severity >= threshold).

    Ordena por severity_score DESC. Mostra score, username, topico,
    alvo, angulo de ataque, citacao e link.
    """
    if not ataques:
        return ""

    sorted_ataques = sorted(
        ataques, key=lambda a: float(a.get("severity_score") or 0), reverse=True
    )

    lines = ["\n<b>ATAQUES CRITICOS</b>"]
    for a in sorted_ataques:
        score = a.get("severity_score", 0)
        user = _escape_html(a.get("username", "?"))
        topic = _escape_html(a.get("primary_topic", "?"))
        target = _escape_html(a.get("target", "?"))
        angle = _escape_html(_truncate(a.get("attack_angle", ""), 120))
        url = a.get("url", "")

        quotes = a.get("key_quotes") or []
        quote = _escape_html(_truncate(quotes[0] if quotes else "", MAX_QUOTE_LEN))

        action = a.get("action_recommendation", "")

        lines.append(
            "\n[{score}/10] @{user}\n"
            "Topico: {topic}\n"
            "Alvo: {target}\n"
            "Angulo: {angle}\n"
            '"{quote}"\n'
            "Acao: <b>{action}</b>\n"
            '<a href="{url}">Ver post</a>'.format(
                score=score, user=user, topic=topic,
                target=target, angle=angle, quote=quote,
                action=_escape_html(action), url=url,
            )
        )

    return "\n".join(lines)


def _format_collabs(collabs: List[Dict[str, Any]]) -> str:
    """
    Formata a secao de oportunidades de collab (amplification >= threshold).

    Ordena por amplification_score DESC.
    """
    if not collabs:
        return ""

    sorted_collabs = sorted(
        collabs, key=lambda c: float(c.get("amplification_score") or 0), reverse=True
    )

    lines = ["\n<b>OPORTUNIDADES DE COLLAB</b>"]
    for c in sorted_collabs:
        score = c.get("amplification_score", 0)
        user = _escape_html(c.get("username", "?"))
        topic = _escape_html(c.get("primary_topic", "?"))
        alignment = _escape_html(_truncate(c.get("alignment", ""), 120))
        url = c.get("url", "")

        quotes = c.get("key_quotes") or []
        quote = _escape_html(_truncate(quotes[0] if quotes else "", MAX_QUOTE_LEN))

        action = c.get("action_recommendation", "")

        lines.append(
            "\n[{score}/10] @{user}\n"
            "Topico: {topic}\n"
            "Alinhamento: {alignment}\n"
            '"{quote}"\n'
            "Acao: <b>{action}</b>\n"
            '<a href="{url}">Ver post</a>'.format(
                score=score, user=user, topic=topic,
                alignment=alignment, quote=quote,
                action=_escape_html(action), url=url,
            )
        )

    return "\n".join(lines)


def _format_resumo(analyses: List[Dict[str, Any]]) -> str:
    """
    Formata o resumo executivo: contagem por tipo, top 3 topicos,
    e recomendacao estrategica baseada no sentimento geral.
    """
    type_counts = Counter()  # type: Counter[str]
    topic_counts = Counter()  # type: Counter[str]

    for a in analyses:
        ct = a.get("content_type", "NEUTRO")
        type_counts[ct] += 1

        pt = a.get("primary_topic", "")
        if pt:
            topic_counts[pt] += 1
        for st in a.get("secondary_topics") or []:
            if st:
                topic_counts[st] += 1

    lines = ["\n<b>RESUMO EXECUTIVO</b>"]

    lines.append(
        "Ataques: {atk} | Collabs: {col} | Propostas: {prop} | "
        "Informativos: {inf} | Neutros: {neu}".format(
            atk=type_counts.get("ATAQUE", 0),
            col=type_counts.get("COLLAB", 0),
            prop=type_counts.get("PROPOSTA", 0),
            inf=type_counts.get("INFORMATIVO", 0),
            neu=type_counts.get("NEUTRO", 0),
        )
    )

    top_topics = topic_counts.most_common(3)
    if top_topics:
        topics_str = ", ".join(
            "{} ({})".format(_escape_html(t), c) for t, c in top_topics
        )
        lines.append("Top topicos: {}".format(topics_str))

    # Recomendacao estrategica
    n_ataques = type_counts.get("ATAQUE", 0)
    n_collabs = type_counts.get("COLLAB", 0)

    if n_ataques > n_collabs and n_ataques > 0:
        lines.append(
            "\n<i>Recomendacao: Predominancia de ataques detectada. "
            "Priorizar monitoramento e preparar respostas estrategicas.</i>"
        )
    elif n_collabs > n_ataques and n_collabs > 0:
        lines.append(
            "\n<i>Recomendacao: Cenario favoravel com oportunidades de "
            "amplificacao. Engajar aliados e reforcar narrativa.</i>"
        )
    else:
        lines.append(
            "\n<i>Recomendacao: Cenario neutro. Manter monitoramento "
            "de rotina e observar tendencias emergentes.</i>"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Funcao principal
# ---------------------------------------------------------------------------


def send_analysis_report(analyses: List[Dict[str, Any]]) -> None:
    """
    Envia relatorio formatado de analise politica via Telegram.

    Filtra ataques criticos (severity >= 7.0) e collabs de alto potencial
    (amplification >= 7.5). So envia se houver pelo menos um item relevante.
    Divide a mensagem se exceder 4096 chars.

    Args:
        analyses: Lista de dicts retornados por classifier.classify_content().
    """
    if not analyses:
        logger.info("REPORT_SKIP: nenhuma analise para reportar")
        return

    creds = _get_credentials()
    if creds is None:
        return
    token, chat_id = creds

    # Filtra ataques e collabs acima do threshold
    ataques = [
        a for a in analyses
        if a.get("content_type") == "ATAQUE"
        and float(a.get("severity_score") or 0) >= SEVERITY_THRESHOLD
    ]

    collabs = [
        a for a in analyses
        if a.get("content_type") == "COLLAB"
        and float(a.get("amplification_score") or 0) >= AMPLIFICATION_THRESHOLD
    ]

    # So envia se houver conteudo relevante
    if not ataques and not collabs:
        logger.info(
            "REPORT_SKIP: nenhum ataque (>= %.1f) ou collab (>= %.1f) encontrado",
            SEVERITY_THRESHOLD, AMPLIFICATION_THRESHOLD,
        )
        return

    # Monta o relatorio
    parts = [_format_header(len(analyses))]

    if ataques:
        parts.append(_format_ataques(ataques))

    if collabs:
        parts.append(_format_collabs(collabs))

    parts.append(_format_resumo(analyses))

    report = "\n".join(parts)

    # Envia (com split automatico se necessario)
    ok = _split_and_send(token, chat_id, report)
    if ok:
        logger.info(
            "REPORT_SENT: %d ataques, %d collabs, %d total, %d chars",
            len(ataques), len(collabs), len(analyses), len(report),
        )
    else:
        logger.error("REPORT_FAILED: falha ao enviar relatorio ao Telegram")


# ---------------------------------------------------------------------------
# Teste de conexao
# ---------------------------------------------------------------------------


def test_report() -> None:
    """
    Envia um relatorio dummy para verificar a conexao com o Telegram.

    Usa dados ficticios cobrindo ATAQUE e COLLAB acima dos thresholds.
    Util para validar tokens e chat_id apos configuracao.
    """
    dummy = [
        {
            "username": "teste_atacante",
            "url": "https://www.instagram.com/p/TESTE1/",
            "primary_topic": "Politica Institucional",
            "secondary_topics": [],
            "content_type": "ATAQUE",
            "severity_score": 8.5,
            "amplification_score": None,
            "confidence_score": 0.9,
            "target": "Renan Santos",
            "attack_angle": "Acusa de elitismo e desconexao com a base",
            "alignment": None,
            "key_quotes": ["Esse cara nunca pisou numa favela na vida"],
            "action_recommendation": "RESPONDER URGENTE",
            "reasoning": "Teste de relatorio - ataque ficticio.",
            "analyzed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "classifier_version": "test",
        },
        {
            "username": "teste_aliado",
            "url": "https://www.instagram.com/p/TESTE2/",
            "primary_topic": "Economia/Fiscal",
            "secondary_topics": ["Liberalismo/Valores"],
            "content_type": "COLLAB",
            "severity_score": None,
            "amplification_score": 9.0,
            "confidence_score": 0.85,
            "target": None,
            "attack_angle": None,
            "alignment": "Ortodoxia fiscal e corte de gastos publicos",
            "key_quotes": ["Precisamos de responsabilidade fiscal, nao de populismo"],
            "action_recommendation": "AMPLIFICAR",
            "reasoning": "Teste de relatorio - collab ficticio.",
            "analyzed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "classifier_version": "test",
        },
        {
            "username": "teste_neutro",
            "url": "https://www.instagram.com/p/TESTE3/",
            "primary_topic": "Outros/Nao Politico",
            "secondary_topics": [],
            "content_type": "NEUTRO",
            "severity_score": None,
            "amplification_score": None,
            "confidence_score": 0.3,
            "target": None,
            "attack_angle": None,
            "alignment": None,
            "key_quotes": [],
            "action_recommendation": "ARQUIVAR",
            "reasoning": "Conteudo sem relevancia politica.",
            "analyzed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "classifier_version": "test",
        },
    ]

    logger.info("TEST_REPORT: enviando relatorio de teste com %d analises", len(dummy))
    send_analysis_report(dummy)


if __name__ == "__main__":
    # Carrega .env manualmente (mesmo padrao do main.py)
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    test_report()
