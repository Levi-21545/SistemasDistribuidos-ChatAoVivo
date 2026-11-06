"""Rede em memória para testar o núcleo do nó sem HTTP.

Cada par (origem, destino) é um canal FIFO, como o CanalFIFO real. O passo de
simulação escolhe um canal ao acaso, então canais diferentes se intercalam de
forma arbitrária. Pode repetir a entrega do mesmo pacote (retry após resposta
perdida) e mantém o pacote no canal quando o destino responde 503.
"""

import copy
import random
from collections import deque

from app.config import Config
from app.modelos import Visao
from app.no import No


class RedeSimulada:
    def __init__(self, ids, rng: random.Random, prob_duplicar: float = 0.0):
        self.rng = rng
        self.prob_duplicar = prob_duplicar
        self.canais: dict[tuple[int, int], deque] = {(o, d): deque() for o in ids for d in ids if o != d}
        self.diretos: list[tuple[int, int, dict]] = []
        self.nos: dict[int, No] = {}
        self.duplicatas = 0
        self.recusas_503 = 0

    def para(self, origem: int) -> "_RedeDoNo":
        return _RedeDoNo(self, origem)

    def ha_pacotes(self) -> bool:
        return any(self.canais.values())

    def passo(self) -> bool:
        ocupados = [c for c, fila in self.canais.items() if fila]
        if not ocupados:
            return False
        origem, destino = self.rng.choice(ocupados)
        fila = self.canais[(origem, destino)]
        pacote = fila[0]
        status, _ = self.nos[destino].receber_pacote(copy.deepcopy(pacote))
        if status == 503:
            self.recusas_503 += 1
            return True
        assert status == 200, (status, pacote)
        if self.rng.random() < self.prob_duplicar:
            self.duplicatas += 1
            status, corpo = self.nos[destino].receber_pacote(copy.deepcopy(pacote))
            assert status == 200
        fila.popleft()
        return True


class _RedeDoNo:
    def __init__(self, rede: RedeSimulada, origem: int):
        self.rede = rede
        self.origem = origem

    def enfileirar(self, destino: int, pacote: dict) -> None:
        self.rede.canais[(self.origem, destino)].append(copy.deepcopy(pacote))

    def enviar_direto(self, destino: int, pacote: dict) -> None:
        self.rede.diretos.append((self.origem, destino, copy.deepcopy(pacote)))

    def tamanhos(self) -> dict[int, int]:
        return {}


def criar_grupo(ids=(1, 2, 3), seed=0, prob_duplicar=0.0, ativos=None):
    """Cria nós com a visão inicial instalada; `ativos` define quem já retomou."""
    rng = random.Random(seed)
    rede = RedeSimulada(ids, rng, prob_duplicar)
    nos_cfg = {i: f"http://no{i}" for i in ids}
    visao = Visao(epoca=(1, max(ids)), lider=max(ids), membros=frozenset(ids))
    for i in ids:
        no = No(Config(no_id=i, nos=nos_cfg), rede.para(i), agora=lambda: 0.0)
        no.instalar(visao, historico=[], relogio=0)
        if ativos is None or i in ativos:
            no.retomar()
        rede.nos[i] = no
    return rede


def mensagem(uuid: str, texto: str, autor: str = "ana") -> dict:
    return {"id": uuid, "sala": "geral", "autor": autor, "texto": texto}


def ids_entregues(no: No) -> list[str]:
    return [e.uuid for e in no.historico]
