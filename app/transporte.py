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
- Os nomes dos nós são resolvidos pelo Resolvedor, no máximo uma busca por vez
  por nó, e os envios usam o último IP conhecido (ver Resolvedor).
"""

import asyncio
import logging
import socket
import time

import httpx

from .config import Config

log = logging.getLogger(__name__)

CAMINHO_PACOTE = "/interno/pacote"


class Resolvedor:
    """Cache de DNS dos nós, com no máximo uma busca em curso por nó.

    Sem ele, cada envio a um nó parado resolvia o nome de novo, e no Docker a
    resolução de um contêiner parado leva ~4 s. Com heartbeats a cada segundo,
    as buscas presas lotavam o pool de resolução (4 threads no uvloop; o timeout
    do pedido não libera a thread), e os controles para nós vivos que precisavam
    de conexão nova (ELECTION, OK, COORDINATOR, VIEW_PREPARE) estouravam o tempo.
    O resultado era o nó 1 se eleger sozinho enquanto o nó 2 esperava por ele.

    Agora o envio usa o último IP conhecido. Uma falha de conexão (o nó pode ter
    voltado com outro IP) ou um IP mais velho que `validade` dispara uma nova
    busca em segundo plano; enquanto ela não termina, vale o IP anterior.
    """

    def __init__(self, nos: dict[int, str], validade: float = 10.0):
        self._bases = {i: httpx.URL(url) for i, url in nos.items()}
        self._validade = validade
        self._ips: dict[int, tuple[str, float]] = {}  # destino -> (IP, momento da busca)
        self._buscas: dict[int, asyncio.Task] = {}

    def url(self, destino: int, caminho: str) -> str | None:
        """URL com o IP conhecido, ou None se o nome ainda não foi resolvido."""
        conhecido = self._ips.get(destino)
        if conhecido is None or time.monotonic() - conhecido[1] > self._validade:
            self.buscar(destino)
        if conhecido is None:
            return None
        return str(self._bases[destino].copy_with(host=conhecido[0], path=caminho))

    def falhou(self, destino: int) -> None:
        self.buscar(destino)

    def buscar(self, destino: int) -> None:
        if destino not in self._buscas:
            tarefa = asyncio.get_running_loop().create_task(self._resolver(destino))
            self._buscas[destino] = tarefa

    async def _resolver(self, destino: int) -> None:
        base = self._bases[destino]
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                base.host, base.port or 80, type=socket.SOCK_STREAM)
            # IPv4 primeiro: o uvicorn local escuta em 127.0.0.1, não em ::1.
            infos.sort(key=lambda info: info[0] != socket.AF_INET)
            self._ips[destino] = (infos[0][4][0], time.monotonic())
        except OSError as erro:
            log.debug("nome do nó %s (%s) não resolvido: %r", destino, base.host, erro)
        finally:
            del self._buscas[destino]

    def parar(self) -> None:
        for tarefa in self._buscas.values():
            tarefa.cancel()


class CanalFIFO:
    def __init__(self, destino: int, url: str, cliente: httpx.AsyncClient, cfg: Config,
                 resolvedor: Resolvedor | None = None):
        self.destino = destino
        self._url = url + CAMINHO_PACOTE
        self._resolvedor = resolvedor
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
            url = self._url if self._resolvedor is None else self._resolvedor.url(self.destino, CAMINHO_PACOTE)
            try:
                if url is None:
                    raise httpx.ConnectError(f"nome do nó {self.destino} ainda não resolvido")
                resposta = await self._cliente.post(url, json=pacote)
                if resposta.status_code == 200:
                    return None
                if resposta.status_code < 500:
                    return f"HTTP {resposta.status_code} {resposta.text}"
                
                log.debug("nó %s respondeu %s a %s; repetindo", self.destino, resposta.status_code,
                          pacote["pacote_id"])
                
            except httpx.HTTPError as erro:
                log.debug("falha ao enviar %s para nó %s: %r", pacote["pacote_id"], self.destino, erro)
                if self._resolvedor is not None and isinstance(erro, (httpx.ConnectError, httpx.ConnectTimeout)):
                    self._resolvedor.falhou(self.destino)
            await asyncio.sleep(espera)
            espera = min(espera * 2, self._cfg.retry_maximo)


class RedeHTTP:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._cliente = httpx.AsyncClient(timeout=cfg.http_timeout)
        self._resolvedor = Resolvedor({i: cfg.nos[i] for i in cfg.pares})
        self._canais = {i: CanalFIFO(i, cfg.nos[i], self._cliente, cfg, self._resolvedor) for i in cfg.pares}
        self._tarefas: set[asyncio.Task] = set()

    def iniciar(self) -> None:
        for destino in self._cfg.pares:
            self._resolvedor.buscar(destino)
        for canal in self._canais.values():
            canal.iniciar()

    async def encerrar(self) -> None:
        self._resolvedor.parar()
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
        url = self._resolvedor.url(destino, CAMINHO_PACOTE)
        try:
            if url is None:
                raise httpx.ConnectError(f"nome do nó {destino} ainda não resolvido")
            resposta = await self._cliente.post(url, json=pacote, timeout=self._cfg.heartbeat_intervalo)
        except Exception as erro:
            if isinstance(erro, (httpx.ConnectError, httpx.ConnectTimeout)):
                self._resolvedor.falhou(destino)
            # O detector de falhas e os reenvios periódicos tratam a ausência; o aviso
            # só torna visível a perda. Heartbeat a nó caído é esperado: fica em debug.
            nivel = logging.DEBUG if pacote.get("tipo") == "HEARTBEAT" else logging.WARNING
            log.log(nivel, "envio de %s (%s) para nó %s falhou: %r", pacote.get("tipo"),
                    pacote.get("pacote_id"), destino, erro)
            return
        if resposta.status_code != 200:
            log.warning("nó %s respondeu %s a %s (%s): %s", destino, resposta.status_code,
                        pacote.get("tipo"), pacote.get("pacote_id"), resposta.text[:200])

    def tamanhos(self) -> dict[int, int]:
        return {i: c.tamanho for i, c in self._canais.items()}
