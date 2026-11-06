"""Transporte HTTP entre nós.

- MSG e ACK passam por um CanalFIFO por destino: um POST por vez, e a fila só
  avança depois de uma resposta 200. Falha de transporte ou 503 repete o mesmo
  pacote (mesmo pacote_id); o receptor deduplica e responde sem repetir efeitos.
- Mensagens de controle (heartbeat, eleição, visão) usam envio direto, com uma
  única tentativa. Elas são idempotentes e reenviadas pela própria lógica do nó.
"""

import asyncio
import logging

import httpx

from .config import Config

log = logging.getLogger(__name__)

CAMINHO_PACOTE = "/interno/pacote"


class CanalFIFO:
    def __init__(self, destino: int, url: str, cliente: httpx.AsyncClient, cfg: Config):
        self.destino = destino
        self._url = url + CAMINHO_PACOTE
        self._cliente = cliente
        self._cfg = cfg
        self._fila: asyncio.Queue[dict] = asyncio.Queue()
        self._tarefa: asyncio.Task | None = None

    def iniciar(self) -> None:
        self._tarefa = asyncio.create_task(self._executar())

    def parar(self) -> None:
        if self._tarefa:
            self._tarefa.cancel()

    def enfileirar(self, pacote: dict) -> None:
        self._fila.put_nowait(pacote)

    @property
    def tamanho(self) -> int:
        return self._fila.qsize()

    async def _executar(self) -> None:
        while True:
            pacote = await self._fila.get()
            await self._entregar(pacote)

    async def _entregar(self, pacote: dict) -> None:
        """Repete o mesmo pacote até o destino responder 200."""
        espera = self._cfg.retry_inicial
        while True:
            try:
                resposta = await self._cliente.post(self._url, json=pacote)
                if resposta.status_code == 200:
                    return
                if resposta.status_code != 503:
                    # 4xx indica pacote malformado (defeito no código): repetir não resolve.
                    log.error("nó %s recusou %s: %s %s", self.destino, pacote["pacote_id"],
                              resposta.status_code, resposta.text)
                    return
            except httpx.HTTPError as erro:
                log.debug("falha ao enviar %s para nó %s: %r", pacote["pacote_id"], self.destino, erro)
            await asyncio.sleep(espera)
            espera = min(espera * 2, self._cfg.retry_maximo)


class RedeHTTP:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._cliente = httpx.AsyncClient(timeout=cfg.http_timeout)
        self._canais = {i: CanalFIFO(i, cfg.nos[i], self._cliente, cfg) for i in cfg.pares}
        self._tarefas: set[asyncio.Task] = set()

    def iniciar(self) -> None:
        for canal in self._canais.values():
            canal.iniciar()

    async def encerrar(self) -> None:
        for canal in self._canais.values():
            canal.parar()
        for tarefa in list(self._tarefas):
            tarefa.cancel()
        await self._cliente.aclose()

    def enfileirar(self, destino: int, pacote: dict) -> None:
        """Envio confiável e FIFO (MSG/ACK)."""
        self._canais[destino].enfileirar(pacote)

    def enviar_direto(self, destino: int, pacote: dict) -> None:
        """Envio de controle: uma tentativa, sem esperar a resposta."""
        tarefa = asyncio.create_task(self._post_unico(destino, pacote))
        self._tarefas.add(tarefa)
        tarefa.add_done_callback(self._tarefas.discard)

    async def _post_unico(self, destino: int, pacote: dict) -> None:
        url = self._cfg.nos[destino] + CAMINHO_PACOTE
        try:
            await self._cliente.post(url, json=pacote, timeout=self._cfg.heartbeat_intervalo)
        except httpx.HTTPError:
            pass  # o detector de falhas e os reenvios periódicos tratam a ausência

    def tamanhos(self) -> dict[int, int]:
        return {i: c.tamanho for i, c in self._canais.items()}
