"""Transporte HTTP entre nós.

- MSG e ACK passam por um CanalFIFO por destino: um POST por vez, e a fila só
  avança depois de uma resposta 200. Falha de transporte, timeout ou 5xx repete
  o mesmo pacote (mesmo pacote_id); o receptor deduplica e responde sem repetir
  efeitos. Qualquer outra resposta (como 400) para o canal naquele pacote, sem
  descartá-lo nem deixar o próximo passar à frente.
- Ao fechar uma visão (corte da reorganização), encerrar_epoca() descarta os
  MSG/ACK daquela época ou anteriores, na fila ou em retry, em todos os canais,
  inclusive o do nó removido. Os pacotes da nova época seguem normalmente.
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
        self.bloqueio: dict | None = None  # pacote recusado e motivo; o canal não avança
        self._encerrada: tuple | None = None  # maior época fechada; pacotes até ela são descartados

    def iniciar(self) -> None:
        self._tarefa = asyncio.create_task(self._executar())

    def parar(self) -> None:
        if self._tarefa:
            self._tarefa.cancel()

    def enfileirar(self, pacote: dict) -> None:
        self._fila.put_nowait(pacote)

    def encerrar_epoca(self, epoca) -> None:
        self._encerrada = tuple(epoca)

    def _antigo(self, pacote: dict) -> bool:
        return self._encerrada is not None and tuple(pacote["epoca"]) <= self._encerrada

    @property
    def tamanho(self) -> int:
        return self._fila.qsize()

    async def _executar(self) -> None:
        while True:
            pacote = await self._fila.get()
            if self._antigo(pacote):
                continue  # visão encerrada: o pacote não é mais enviado
            motivo = await self._entregar(pacote)
            if motivo:
                # Recusa permanente (defeito no código): repetir não resolve, e pular o
                # pacote quebraria o FIFO. O canal para aqui; os outros destinos seguem.
                self.bloqueio = {"pacote": pacote, "motivo": motivo}
                log.error("canal para o nó %s parado em %s: %s", self.destino, pacote["pacote_id"], motivo)
                return

    async def _entregar(self, pacote: dict) -> str | None:
        """Repete o mesmo pacote até o destino responder 200.

        Devolve None no sucesso ou o motivo de uma recusa permanente.
        """
        espera = self._cfg.retry_inicial
        while True:
            if self._antigo(pacote):
                log.info("retry de %s para nó %s encerrado: visão %s fechada", pacote["pacote_id"],
                         self.destino, pacote["epoca"])
                return None
            try:
                resposta = await self._cliente.post(self._url, json=pacote)
                if resposta.status_code == 200:
                    return None
                if resposta.status_code < 500:
                    return f"HTTP {resposta.status_code} {resposta.text}"
                
                log.debug("nó %s respondeu %s a %s; repetindo", self.destino, resposta.status_code,
                          pacote["pacote_id"])
                
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

    def encerrar_epoca(self, epoca) -> None:
        """Descarta MSG/ACK da época fechada (e anteriores) em todos os canais."""
        for canal in self._canais.values():
            canal.encerrar_epoca(epoca)

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
