"""Rede em memória para testar o núcleo do nó sem HTTP.

Cada par (origem, destino) é um canal FIFO, como o CanalFIFO real. O passo de
simulação escolhe um canal ao acaso, então canais diferentes se intercalam de
forma arbitrária. Pode repetir a entrega do mesmo pacote (retry após resposta
perdida) e mantém o pacote no canal quando o destino responde 503.

Para a reorganização (T05): nós em `mortos` não recebem nem enviam (o que já
estava no canal continua em trânsito), `tempo` é o relógio dos nós, controles
diretos são entregues por entregar_diretos(), que pode perder pacotes, e
encerrar_epoca() retira do canal os MSG/ACK da época fechada, como o CanalFIFO.
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
        self.mortos: set[int] = set()
        self.tempo = 0.0
        self.perder = None  # função (origem, destino, pacote) -> bool para descartar controles
        self.segurados: set[tuple[int, int]] = set()  # canais em trânsito que passo() não entrega
        self.controles: list[tuple[int, int, dict]] = []  # controles entregues, para reenviar depois
        self.respostas: list[tuple[int, dict, int, dict]] = []  # (destino, pacote, status, corpo) dos dados

    def para(self, origem: int) -> "_RedeDoNo":
        return _RedeDoNo(self, origem)

    def ha_pacotes(self) -> bool:
        return any(self.canais.values())

    def passo(self) -> bool:
        ocupados = [c for c, fila in self.canais.items()
                    if fila and c[1] not in self.mortos and c not in self.segurados]
        if not ocupados:
            return False
        return self.entregar(*self.rng.choice(ocupados))

    def entregar(self, origem: int, destino: int) -> bool:
        """Entrega o primeiro pacote do canal (origem, destino), como o CanalFIFO."""
        fila = self.canais[(origem, destino)]
        pacote = fila[0]
        status, corpo = self.nos[destino].receber_pacote(copy.deepcopy(pacote))
        self.respostas.append((destino, pacote, status, corpo))
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


    def entregar_diretos(self) -> int:
        """Entrega os controles pendentes (exceto de/para nós mortos ou perdidos)."""
        diretos, self.diretos = self.diretos, []
        for origem, destino, pacote in diretos:
            if origem in self.mortos or destino in self.mortos:
                continue
            if self.perder is not None and self.perder(origem, destino, pacote):
                continue
            self.controles.append((origem, destino, copy.deepcopy(pacote)))
            status, corpo = self.nos[destino].receber_pacote(pacote)
            assert status == 200, (status, corpo, pacote)
        return len(diretos)

    def drenar(self, limite: int = 5000) -> None:
        """Entrega controles e dados até não haver progresso (um 503 deixa o pacote no canal)."""
        for _ in range(limite):
            entregou = self.entregar_diretos()
            if not self.passo() and not entregou and not self.diretos:
                return


class _RedeDoNo:
    def __init__(self, rede: RedeSimulada, origem: int):
        self.rede = rede
        self.origem = origem

    def enfileirar(self, destino: int, pacote: dict) -> None:
        self.rede.canais[(self.origem, destino)].append(copy.deepcopy(pacote))

    def enviar_direto(self, destino: int, pacote: dict) -> None:
        self.rede.diretos.append((self.origem, destino, copy.deepcopy(pacote)))

    def encerrar_epoca(self, epoca) -> None:
        for (origem, destino), fila in self.rede.canais.items():
            if origem == self.origem:
                restantes = [p for p in fila if tuple(p["epoca"]) > tuple(epoca)]
                fila.clear()
                fila.extend(restantes)

    def tamanhos(self) -> dict[int, int]:
        return {}


def criar_grupo(ids=(1, 2, 3), seed=0, prob_duplicar=0.0, ativos=None, **tempos):
    """Cria nós com a visão inicial instalada; `ativos` define quem já retomou."""
    rng = random.Random(seed)
    rede = RedeSimulada(ids, rng, prob_duplicar)
    nos_cfg = {i: f"http://no{i}" for i in ids}
    visao = Visao(epoca=(1, max(ids)), lider=max(ids), membros=frozenset(ids))
    for i in ids:
        no = No(Config(no_id=i, nos=nos_cfg, **tempos), rede.para(i), agora=lambda: rede.tempo)
        no.instalar(visao, historico=[], relogio=0)
        if ativos is None or i in ativos:
            no.retomar()
        rede.nos[i] = no
    return rede


def mensagem(uuid: str, texto: str, autor: str = "ana") -> dict:
    return {"id": uuid, "sala": "geral", "autor": autor, "texto": texto}


def ids_entregues(no: No) -> list[str]:
    return [e.uuid for e in no.historico]
