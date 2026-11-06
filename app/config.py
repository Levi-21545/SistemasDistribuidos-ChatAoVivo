"""Configuração do nó, lida de variáveis de ambiente.

NO_ID   identificador deste nó (1, 2 ou 3).
NOS     lista "id=url" separada por vírgula, por exemplo
        "1=http://no1:8000,2=http://no2:8000,3=http://no3:8000".
        No Compose, os nomes são resolvidos pelo DNS; não há IPs no código.

Os tempos (segundos) são opcionais e têm os valores do protocolo como padrão.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    no_id: int
    nos: dict[int, str]
    heartbeat_intervalo: float = 1.0
    suspeita_timeout: float = 3.0
    eleicao_timeout: float = 1.0
    coordenador_timeout: float = 3.0
    http_timeout: float = 2.0
    retry_inicial: float = 0.1
    retry_maximo: float = 1.0

    @property
    def pares(self) -> list[int]:
        """IDs dos outros nós configurados."""
        return sorted(i for i in self.nos if i != self.no_id)


def ler_nos(texto: str) -> dict[int, str]:
    """Converte "1=http://no1:8000,2=http://no2:8000" em {1: url, 2: url}."""
    nos = {}
    for item in texto.split(","):
        item = item.strip()
        if not item:
            continue
        ident, url = item.split("=", 1)
        nos[int(ident)] = url.strip().rstrip("/")
    return nos


def _tempo(nome: str, padrao: float) -> float:
    return float(os.environ.get(nome, padrao))


def carregar() -> Config:
    no_id = int(os.environ["NO_ID"])
    nos = ler_nos(os.environ["NOS"])
    if no_id not in nos:
        raise ValueError(f"NO_ID={no_id} não aparece em NOS")
    return Config(
        no_id=no_id,
        nos=nos,
        heartbeat_intervalo=_tempo("HEARTBEAT_INTERVALO", 1.0),
        suspeita_timeout=_tempo("SUSPEITA_TIMEOUT", 3.0),
        eleicao_timeout=_tempo("ELEICAO_TIMEOUT", 1.0),
        coordenador_timeout=_tempo("COORDENADOR_TIMEOUT", 3.0),
        http_timeout=_tempo("HTTP_TIMEOUT", 2.0),
    )
