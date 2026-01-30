from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

CLASSIFIER_VERSION = "v1.3"
MODEL_NAME = "gpt-4o-mini"
MODEL_TEMPERATURE = 0.2
MODEL_MAX_TOKENS = 1000
MAX_TRANSCRIPT_CHARS = 15000
MAX_RETRIES = 3
BACKOFF_SECONDS = [0.5, 1.0, 2.0]
CACHE_PATH = "classifier_cache.json"

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logger = logging.getLogger("classifier")
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
# Lock para acesso concorrente ao cache
# ---------------------------------------------------------------------------

_cache_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Metricas de uso da API (in-process)
# ---------------------------------------------------------------------------

_api_stats_lock = threading.Lock()
_api_total_calls: int = 0
_api_total_latency: float = 0.0


def get_api_stats() -> Dict[str, Any]:
    """Retorna estatisticas acumuladas de chamadas a API (calls, latencia media)."""
    with _api_stats_lock:
        avg = (
            round(_api_total_latency / _api_total_calls, 2)
            if _api_total_calls > 0
            else 0.0
        )
        return {
            "total_calls": _api_total_calls,
            "total_latency": round(_api_total_latency, 2),
            "avg_latency": avg,
        }


def _record_api_call(latency: float) -> None:
    """Registra uma chamada a API para fins de observabilidade."""
    global _api_total_calls, _api_total_latency
    with _api_stats_lock:
        _api_total_calls += 1
        _api_total_latency += latency


# ---------------------------------------------------------------------------
# Constantes de validacao
# ---------------------------------------------------------------------------

VALID_TOPICS: List[str] = [
    "Economia/Fiscal",
    "Segurança Pública",
    "Reforma Urbana/Habitação",
    "Política Institucional",
    "Geração Z/Juventude",
    "Liberalismo/Valores",
    "Justiça/Judiciário",
    "Corrupção/Escândalos",
    "Mídia/Narrativa",
    "Política Local",
    "Direitos Sociais/Minorias",
    "Outros/Não Político",
]

VALID_CONTENT_TYPES: List[str] = [
    "ATAQUE", "COLLAB", "PROPOSTA", "INFORMATIVO", "NEUTRO",
]

VALID_ACTIONS: List[str] = [
    "RESPONDER URGENTE",
    "MONITORAR",
    "AMPLIFICAR",
    "PARCERIA",
    "ANALISAR",
    "ARQUIVAR",
]

# Chaves obrigatorias no resultado final (evita KeyError no main.py)
REQUIRED_KEYS: Dict[str, Any] = {
    "username": None,
    "url": None,
    "primary_topic": "Outros/Não Político",
    "secondary_topics": [],
    "content_type": "NEUTRO",
    "severity_score": None,
    "amplification_score": None,
    "confidence_score": 0.5,
    "target": None,
    "attack_angle": None,
    "alignment": None,
    "proposal_summary": None,
    "alignment_status": None,
    "info_summary": None,
    "key_quotes": [],
    "action_recommendation": "ARQUIVAR",
    "reasoning": "",
    "analyzed_at": None,
    "classifier_version": CLASSIFIER_VERSION,
    "latency_seconds": None,
}

# ---------------------------------------------------------------------------
# System prompt (conteudo politico inalterado)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Voce e um analista politico especializado no cenario brasileiro atual. Sua tarefa e classificar transcricoes de Reels do Instagram sob a otica de Renan Santos e do Partido Missao/MBL.

CONTEXTO POLITICO:
- Renan Santos lidera o Partido Missao (ex-MBL), posicionado como terceira via de direita.
- E CRITICO tanto de Lula/PT (esquerda) quanto de Bolsonaro/PL (populismo de direita).
- Bandeiras principais: ortodoxia fiscal, reforma urbana ("acabar com favelas"), guerra ao crime organizado/faccoes, conquista da Geracao Z.
- Valores: liberalismo economico, meritocracia, antipopulismo, rigor institucional.

CLASSIFICACAO DE TOPICO:
Identifique UM topico principal (primary_topic) e 0-2 topicos secundarios (secondary_topics) desta lista EXATA:
"Economia/Fiscal", "Segurança Pública", "Reforma Urbana/Habitação", "Política Institucional", "Geração Z/Juventude", "Liberalismo/Valores", "Justiça/Judiciário", "Corrupção/Escândalos", "Mídia/Narrativa", "Política Local", "Direitos Sociais/Minorias", "Outros/Não Político".

TIPO DE CONTEUDO (escolha UM):

ATAQUE - Conteudo que critica/ataca Renan, Missao, MBL ou seus valores. Inclui defesa de Lula/PT ou Bolsonaro/bolsonarismo, promocao de populismo.
Sinais: "Renan/MBL e...", "terceira via fracassou", "liberal traidor", "apoio ao Lula/Bolsonaro".
Extraia: target (pessoa/instituicao), attack_angle (descricao da critica), key_quotes (2-3 citacoes diretas).
severity_score (0.0-10.0): baseado em potencial de alcance, direcionamento do ataque (vago=3.0-5.0, direto=7.0-9.0, difamacao=9.0-10.0), sensibilidade do topico, presenca de call-to-action (+2.0).

COLLAB - Conteudo que apoia posicoes de Renan, critica Lula E Bolsonaro, promove ortodoxia fiscal, reforma urbana, combate ao crime, liberalismo.
Sinais: "terceira via", "nem Lula nem Bolsonaro", "ajuste fiscal", "fim das favelas", "combate ao crime organizado".
Extraia: alignment (qual valor/politica de Renan apoia), key_quotes (2-3 citacoes).
amplification_score (0.0-10.0): baseado em alinhamento estrategico (politicas centrais=8.0-10.0, tangencial=4.0-6.0), credibilidade do influencer, novidade do argumento, potencial viral.

PROPOSTA - Sugestoes concretas de politica publica.
Extraia: proposal_summary (1 frase), alignment_status ("aligned"|"partially_aligned"|"opposed"), key_quotes (1-2 citacoes).

INFORMATIVO - Noticias, dados, fatos sem opiniao clara.
Extraia: info_summary (1 frase).

NEUTRO - Conteudo nao politico (lifestyle, entretenimento, pessoal).

SCORES:
- confidence_score (0.0-1.0): linguagem politica clara=0.8-1.0, ambiguo=0.4-0.7, muito curto/confuso=0.1-0.4.
- severity_score e amplification_score: escala 0.0-10.0, uma casa decimal.

RECOMENDACAO DE ACAO:
- "RESPONDER URGENTE": ataque grave (severity>7.0), precisa resposta imediata.
- "MONITORAR": ataque de baixa severidade.
- "AMPLIFICAR": collab forte, compartilhar amplamente.
- "PARCERIA": collab, buscar colaboracao.
- "ANALISAR": proposta, precisa revisao da equipe.
- "ARQUIVAR": informativo/neutro, sem acao necessaria.

REASONING: Explique em 2-3 frases POR QUE classificou assim. Inclua sinais que acionaram a classificacao, contexto da pontuacao, e incertezas.

Voce DEVE responder com um unico objeto JSON valido (sem markdown, sem texto fora do JSON). Use este formato exato:
{
  "primary_topic": "...",
  "secondary_topics": [],
  "content_type": "ATAQUE|COLLAB|PROPOSTA|INFORMATIVO|NEUTRO",
  "severity_score": null,
  "amplification_score": null,
  "confidence_score": 0.0,
  "target": null,
  "attack_angle": null,
  "alignment": null,
  "proposal_summary": null,
  "alignment_status": null,
  "info_summary": null,
  "key_quotes": [],
  "action_recommendation": "...",
  "reasoning": "..."
}

Preencha APENAS os campos relevantes para o content_type classificado. Campos nao aplicaveis devem ser null ou lista vazia.\
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hash_transcript(transcript: str) -> str:
    """Retorna o SHA-256 hex do transcript (usado como identificador seguro)."""
    return hashlib.sha256(transcript.strip().encode("utf-8")).hexdigest()


def _load_cache() -> Dict[str, Any]:
    """
    Carrega o cache de classificacoes do disco.

    Thread-safe via _cache_lock. Retorna dict vazio se o arquivo
    nao existir ou estiver corrompido.
    """
    with _cache_lock:
        if not os.path.exists(CACHE_PATH):
            return {}
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("CACHE_READ_ERROR: %s", e)
            return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    """
    Salva o cache no disco de forma atomica e thread-safe.

    Escreve em arquivo temporario e faz os.replace (atomico no OS)
    para evitar corrupcao em concorrencia entre processos.
    O _cache_lock protege contra concorrencia entre threads.
    """
    tmp_path: Optional[str] = None
    with _cache_lock:
        try:
            dir_name = os.path.dirname(os.path.abspath(CACHE_PATH))
            fd, tmp_path = tempfile.mkstemp(suffix=".tmp", dir=dir_name)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, CACHE_PATH)
        except OSError as e:
            logger.error("CACHE_WRITE_ERROR: %s", e)
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


def _extract_json(raw: str) -> Optional[Dict[str, Any]]:
    """
    Extrai o primeiro objeto JSON valido do texto retornado pelo LLM.

    Estrategia em 3 etapas:
    1. Parse direto apos remover fences markdown.
    2. Busca o primeiro bloco { ... } com chaves balanceadas.
    3. Regex guloso para blocos JSON menores.
    """
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()

    # Tentativa 1: parse direto
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Tentativa 2: bloco balanceado
    start = cleaned.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(cleaned)):
            if cleaned[i] == "{":
                depth += 1
            elif cleaned[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(cleaned[start:i + 1])
                    except json.JSONDecodeError:
                        break

    # Tentativa 3: regex
    for match in re.finditer(r"\{(?:.|\n)*?\}", cleaned):
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            continue

    return None


def _build_api_kwargs(user_message: str) -> Dict[str, Any]:
    """
    Monta os kwargs para a chamada client.chat.completions.create.

    Tenta incluir response_format=json_object. Se o SDK nao suportar
    (versoes antigas da lib openai), faz fallback sem o parametro
    e depende do _extract_json para parsear a resposta.
    """
    return {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "temperature": MODEL_TEMPERATURE,
        "max_tokens": MODEL_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }


def _call_openai(
    api_key: str, user_message: str
) -> Tuple[Optional[str], float, int]:
    """
    Chama a OpenAI com retries e backoff exponencial.

    Trata erros especificos do SDK (rate limit, autenticacao, rede)
    com mensagens de log distintas. Se response_format nao for suportado,
    faz fallback automatico sem o parametro.

    Retorna:
        (resposta_texto, latencia_segundos, total_tokens).
        Em caso de falha apos todas tentativas: (None, latencia_acum, 0).
    """
    client = OpenAI(api_key=api_key)
    total_latency = 0.0
    kwargs = _build_api_kwargs(user_message)
    use_json_format = True

    for attempt in range(MAX_RETRIES):
        t0 = time.monotonic()
        try:
            if not use_json_format:
                call_kwargs = {k: v for k, v in kwargs.items()
                               if k != "response_format"}
            else:
                call_kwargs = kwargs

            response = client.chat.completions.create(**call_kwargs)
            latency = round(time.monotonic() - t0, 2)

            tokens = 0
            if response.usage:
                tokens = response.usage.total_tokens

            text = response.choices[0].message.content.strip()

            _record_api_call(latency)
            stats = get_api_stats()
            logger.info(
                "API_OK: latency=%.2fs tokens=%d attempt=%d "
                "| total_calls=%d avg_latency=%.2fs",
                latency, tokens, attempt + 1,
                stats["total_calls"], stats["avg_latency"],
            )
            return text, latency, tokens

        except Exception as e:
            elapsed = time.monotonic() - t0
            total_latency += elapsed
            error_type = type(e).__name__
            error_msg = str(e)

            # Deteccao de erros especificos do SDK openai
            is_rate_limit = "RateLimitError" in error_type or "429" in error_msg
            is_auth = "AuthenticationError" in error_type or "401" in error_msg
            is_bad_request = "BadRequestError" in error_type or "400" in error_msg

            if is_auth:
                logger.error(
                    "API_AUTH_ERROR: OPENAI_API_KEY invalida ou expirada"
                )
                return None, round(total_latency, 2), 0

            # Fallback: se o modelo/SDK nao suporta response_format
            if is_bad_request and "response_format" in error_msg:
                logger.warning(
                    "API_JSON_MODE_UNSUPPORTED: removendo response_format, "
                    "usando fallback de extracao JSON"
                )
                use_json_format = False
                continue

            if is_rate_limit:
                tag = "API_RATE_LIMIT"
            else:
                tag = "API_ERROR"

            if attempt < MAX_RETRIES - 1:
                wait = BACKOFF_SECONDS[attempt]
                logger.warning(
                    "%s: attempt=%d/%d error=%s wait=%.1fs",
                    tag, attempt + 1, MAX_RETRIES, error_type, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "API_FAILED: attempts=%d error=%s msg=%s",
                    MAX_RETRIES, error_type, e,
                )
                return None, round(total_latency, 2), 0

    return None, round(total_latency, 2), 0


def _ensure_keys(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que todas as chaves obrigatorias existam no resultado.

    Preenche chaves ausentes com valores default de REQUIRED_KEYS,
    evitando KeyError no codigo consumidor (main.py, telegram_reporter).
    """
    for key, default in REQUIRED_KEYS.items():
        if key not in result:
            if isinstance(default, list):
                result[key] = []
            else:
                result[key] = default
    return result


# ---------------------------------------------------------------------------
# Funcao principal
# ---------------------------------------------------------------------------


def classify_content(
    username: str, transcript: str, url: str
) -> Optional[Dict[str, Any]]:
    """
    Classifica o conteudo politico de uma transcricao de Reel usando OpenAI API.

    Args:
        username: Handle do Instagram (sem @).
        transcript: Texto transcrito do video.
        url: URL do post no Instagram.

    Returns:
        Dict com a analise completa ou None em caso de erro irrecuperavel.

    Notas:
        - Nunca loga o texto da transcricao; usa o hash SHA-256 como ID.
        - Resultados sao cacheados por hash do transcript em disco.
        - Thread-safe para leitura/escrita do cache.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("MISSING_API_KEY: OPENAI_API_KEY nao definido")
        return None

    if not transcript or not transcript.strip():
        logger.warning("EMPTY_TRANSCRIPT: user=@%s", username)
        return None

    transcript_clean = transcript.strip()
    t_hash = _hash_transcript(transcript_clean)
    log_id = "user=@{} hash={}".format(username, t_hash[:12])

    # Cache lookup
    cache = _load_cache()
    if t_hash in cache:
        cached = cache[t_hash].copy()
        cached["username"] = username
        cached["url"] = url
        cached["analyzed_at"] = cached.get(
            "analyzed_at", datetime.now(timezone.utc).isoformat()
        )
        logger.info("CACHE_HIT: %s", log_id)
        return _ensure_keys(cached)

    logger.info("CLASSIFY_START: %s chars=%d", log_id, len(transcript_clean))

    # Truncamento
    truncated = False
    if len(transcript_clean) > MAX_TRANSCRIPT_CHARS:
        transcript_clean = transcript_clean[:MAX_TRANSCRIPT_CHARS]
        truncated = True
        logger.info("TRANSCRIPT_TRUNCATED: %s", log_id)

    user_message = (
        "Perfil: @{}\nURL: {}\nTranscricao:\n{}".format(
            username, url, transcript_clean
        )
    )
    if truncated:
        user_message += "\n[[TRUNCATED]]"

    # Chamada API com retries
    raw, latency, tokens = _call_openai(api_key, user_message)
    if raw is None:
        logger.error("API_TIMEOUT: %s latency=%.2fs", log_id, latency)
        return None

    # Parse JSON
    result = _extract_json(raw)
    if result is None:
        logger.error("INVALID_JSON_STRUCTURE: %s", log_id)
        return None

    # Metadados
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    result["username"] = username
    result["url"] = url
    result["analyzed_at"] = now_iso
    result["classifier_version"] = CLASSIFIER_VERSION
    result["latency_seconds"] = latency

    # Validacao de topicos
    if result.get("primary_topic") not in VALID_TOPICS:
        result["primary_topic"] = "Outros/Não Político"

    result["secondary_topics"] = [
        t for t in (result.get("secondary_topics") or [])
        if t in VALID_TOPICS
    ][:2]

    # Validacao de content_type e action
    if result.get("content_type") not in VALID_CONTENT_TYPES:
        result["content_type"] = "NEUTRO"

    if result.get("action_recommendation") not in VALID_ACTIONS:
        result["action_recommendation"] = "ARQUIVAR"

    # Scores float 0.0-10.0 (severity/amplification) e 0.0-1.0 (confidence)
    ct = result["content_type"]

    if ct == "ATAQUE":
        try:
            result["severity_score"] = round(
                max(0.0, min(10.0, float(result.get("severity_score") or 0))),
                1,
            )
        except (TypeError, ValueError):
            result["severity_score"] = 0.0
        result["amplification_score"] = None
    elif ct == "COLLAB":
        try:
            result["amplification_score"] = round(
                max(0.0, min(10.0, float(result.get("amplification_score") or 0))),
                1,
            )
        except (TypeError, ValueError):
            result["amplification_score"] = 0.0
        result["severity_score"] = None
    else:
        result["severity_score"] = None
        result["amplification_score"] = None

    try:
        result["confidence_score"] = round(
            max(0.0, min(1.0, float(result.get("confidence_score") or 0.5))),
            2,
        )
    except (TypeError, ValueError):
        result["confidence_score"] = 0.5

    # key_quotes: max 3, cada truncada em 200 chars
    quotes = result.get("key_quotes") or []
    result["key_quotes"] = [str(q).strip()[:200] for q in quotes][:3]

    # Garante todas as chaves obrigatorias
    result = _ensure_keys(result)

    # Salva no cache (sem username/url que sao contextuais)
    cache_entry = {k: v for k, v in result.items() if k not in ("username", "url")}
    cache[t_hash] = cache_entry
    _save_cache(cache)

    logger.info(
        "CLASSIFY_DONE: %s type=%s confidence=%.2f latency=%.2fs tokens=%d",
        log_id, ct, result["confidence_score"], latency, tokens,
    )

    return result
