import os
import tempfile

import requests
from moviepy import VideoFileClip
from openai import OpenAI


def transcribe_video(media_url: str) -> str | None:
    """
    Baixa o vídeo de media_url, extrai o áudio com moviepy,
    transcreve com OpenAI Whisper API e retorna o texto.
    Retorna None se falhar em qualquer etapa.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY nao definido, pulando transcricao.")
        return None

    tmp_video_path = None
    tmp_audio_path = None
    clip = None

    try:
        # 1. Baixa o vídeo para arquivo temporário
        resp = requests.get(media_url, timeout=120)
        resp.raise_for_status()

        fd, tmp_video_path = tempfile.mkstemp(suffix=".mp4")
        os.write(fd, resp.content)
        os.close(fd)

        # 2. Extrai áudio com moviepy
        tmp_audio_path = tmp_video_path.replace(".mp4", ".mp3")
        clip = VideoFileClip(tmp_video_path)

        if clip.audio is None:
            print("  Video sem audio, pulando.")
            return None

        clip.audio.write_audiofile(tmp_audio_path, logger=None)
        clip.close()
        clip = None

        # 3. Transcreve com Whisper API
        client = OpenAI(api_key=api_key)
        with open(tmp_audio_path, "rb") as audio_file:
            result = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
            )

        return result.text

    except Exception as e:
        print(f"Erro na transcricao: {e}")
        return None

    finally:
        if clip is not None:
            try:
                clip.close()
            except Exception:
                pass
        for path in (tmp_video_path, tmp_audio_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
