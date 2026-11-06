"""Aplicação FastAPI de um nó.

POST /interno/pacote   pacotes entre nós (MSG, ACK, HEARTBEAT, eleição, visão)
WS   /ws               clientes do chat
GET  /estado           estado do nó (depuração e futuro painel)
GET  /historico        histórico entregue, na ordem total
GET  /                 cliente web mínimo
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from .config import carregar
from .modelos import Evento
from .no import No
from .transporte import CAMINHO_PACOTE, RedeHTTP

cfg = carregar()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s nó {cfg.no_id} %(levelname)s %(message)s",
)

rede = RedeHTTP(cfg)
no = No(cfg, rede)
clientes: set[asyncio.Queue] = set()
PAGINA = Path(__file__).resolve().parent.parent / "static" / "index.html"


def avisar_clientes(evento: Evento, posicao: int) -> None:
    aviso = {"tipo": "entregue", "posicao": posicao, **evento.json()}
    for saida in clientes:
        saida.put_nowait(aviso)


no.ouvintes.append(avisar_clientes)


@asynccontextmanager
async def ciclo_de_vida(_app: FastAPI):
    rede.iniciar()
    ciclos = asyncio.create_task(no.executar_ciclos())
    yield
    ciclos.cancel()
    await rede.encerrar()


app = FastAPI(title=f"Chat com ordenação - nó {cfg.no_id}", lifespan=ciclo_de_vida)


@app.post(CAMINHO_PACOTE)
async def receber_pacote(request: Request) -> JSONResponse:
    try:
        pacote = await request.json()
    except ValueError:
        return JSONResponse({"erro": "JSON inválido"}, status_code=400)
    if not isinstance(pacote, dict):
        return JSONResponse({"erro": "o pacote deve ser um objeto"}, status_code=400)
    status, corpo = no.receber_pacote(pacote)
    return JSONResponse(corpo, status_code=status)


@app.get("/estado")
async def estado() -> dict:
    return no.estado_publico()


@app.get("/historico")
async def historico() -> list[dict]:
    return [e.json() for e in no.historico]


@app.get("/")
async def pagina() -> FileResponse:
    return FileResponse(PAGINA)


@app.websocket("/ws")
async def cliente_ws(ws: WebSocket) -> None:
    await ws.accept()
    saida: asyncio.Queue = asyncio.Queue()
    # Histórico e inscrição no mesmo passo síncrono: nenhuma entrega se perde ou repete.
    saida.put_nowait({"tipo": "historico", "no": cfg.no_id, "mensagens": [e.json() for e in no.historico]})
    clientes.add(saida)
    enviador = asyncio.create_task(_enviar_ao_cliente(ws, saida))
    try:
        while True:
            texto = await ws.receive_text()
            try:
                dados = json.loads(texto)
            except ValueError:
                dados = None
            saida.put_nowait(no.aceitar_do_cliente(dados))
    except WebSocketDisconnect:
        pass
    finally:
        clientes.discard(saida)
        enviador.cancel()


async def _enviar_ao_cliente(ws: WebSocket, saida: asyncio.Queue) -> None:
    while True:
        await ws.send_json(await saida.get())
