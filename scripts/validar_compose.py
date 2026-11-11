"""Validação reproduzível no Docker Compose: queda do líder e queda de membro comum.

Uso, na raiz do projeto, com a .venv de requirements-dev.txt:

    .venv/bin/python scripts/validar_compose.py           # os dois cenários
    .venv/bin/python scripts/validar_compose.py lider     # só a queda do líder (nó 3)
    .venv/bin/python scripts/validar_compose.py membro    # só a queda de membro comum (nó 1)

Cada cenário é independente: sobe o projeto do zero (docker compose up -d --build),
conecta um cliente WebSocket a cada nó, envia mensagens concorrentes, derruba um nó
com docker compose kill, espera a nova visão, envia uma mensagem nova e compara os
históricos. No fim, sempre docker compose down. Qualquer falha encerra com código 1.
Gerencia somente os contêineres deste compose.yaml; as portas 8001-8003 precisam
estar livres.
"""

import asyncio
import json
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
from websockets.asyncio.client import connect

RAIZ = Path(__file__).resolve().parent.parent
PORTAS = {1: 8001, 2: 8002, 3: 8003}
URLS = {i: f"http://127.0.0.1:{p}" for i, p in PORTAS.items()}
POR_CLIENTE = 15
CENARIOS = {
    "lider": {"cai": 3, "visao": {"epoca": [2, 2], "lider": 2, "membros": [1, 2]}},
    "membro": {"cai": 1, "visao": {"epoca": [2, 3], "lider": 3, "membros": [2, 3]}},
}


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=RAIZ, check=True, stdout=subprocess.DEVNULL)


def estado(i: int) -> dict:
    return httpx.get(URLS[i] + "/estado", timeout=2).json()


def historico(i: int) -> list[str]:
    return [e["mensagem"]["id"] for e in httpx.get(URLS[i] + "/historico", timeout=2).json()]


def esperar(descricao: str, condicao, limite: float = 30.0) -> float:
    inicio = time.monotonic()
    while time.monotonic() - inicio < limite:
        try:
            if condicao():
                return time.monotonic() - inicio
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"tempo esgotado ({limite:.0f}s): {descricao}")


def msg(texto: str) -> dict:
    return {"id": str(uuid.uuid4()), "sala": "geral", "autor": "validacao", "texto": texto}


async def entregas(ws, quantidade: int, limite: float = 30.0) -> list[str]:
    ids = []
    async with asyncio.timeout(limite):
        while len(ids) < quantidade:
            dados = json.loads(await ws.recv())
            if dados["tipo"] == "entregue":
                ids.append(dados["mensagem"]["id"])
            elif dados["tipo"] == "recusada":
                raise AssertionError(f"mensagem recusada: {dados}")
    return ids


async def conectar(nos) -> dict:
    conexoes = {}
    for i in nos:
        conexoes[i] = await connect(URLS[i].replace("http://", "ws://") + "/ws")
        assert json.loads(await conexoes[i].recv())["tipo"] == "historico"
    return conexoes


async def concorrentes() -> dict[int, list[str]]:
    conexoes = await conectar(PORTAS)
    total = POR_CLIENTE * len(conexoes)
    coletores = {i: asyncio.create_task(entregas(ws, total)) for i, ws in conexoes.items()}

    async def disparar(i, ws):
        for k in range(POR_CLIENTE):
            await ws.send(json.dumps(msg(f"no{i}-m{k}")))

    await asyncio.gather(*(disparar(i, ws) for i, ws in conexoes.items()))
    recebido = {i: await tarefa for i, tarefa in coletores.items()}
    for ws in conexoes.values():
        await ws.close()
    return recebido


async def mensagem_nova(vivos) -> tuple[str, dict[int, list[str]]]:
    conexoes = await conectar(vivos)
    nova = msg("depois da retomada")
    await conexoes[vivos[0]].send(json.dumps(nova))
    recebido = {i: await entregas(ws, 1) for i, ws in conexoes.items()}
    for ws in conexoes.values():
        await ws.close()
    return nova["id"], recebido


def portas_livres() -> None:
    for porta in PORTAS.values():
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", porta)) == 0:
                raise SystemExit(f"porta {porta} ocupada; encerre o que estiver nela antes da validação")


def executar(nome: str) -> None:
    cenario = CENARIOS[nome]
    cai, vivos = cenario["cai"], [i for i in PORTAS if i != cenario["cai"]]
    print(f"\n== cenário {nome}: derrubar o nó {cai} ==")
    portas_livres()
    compose("up", "-d", "--build")
    try:
        t = esperar("três nós ativos na visão (1,3)", lambda: all(
            estado(i)["estado"] == "ativo" and estado(i)["visao"]["epoca"] == [1, 3] for i in PORTAS))
        print(f"três nós ativos na visão (1,3) em {t:.1f}s")

        recebido = asyncio.run(concorrentes())
        assert recebido[1] == recebido[2] == recebido[3], "clientes receberam ordens diferentes"
        assert len(set(recebido[1])) == POR_CLIENTE * 3
        esperar("históricos iguais antes da queda", lambda: historico(1) == historico(2) == historico(3))
        antes = historico(vivos[0])
        print(f"{len(antes)} mensagens concorrentes, mesma sequência nos três clientes e históricos")

        compose("kill", f"no{cai}")
        t = esperar(f"sobreviventes {vivos} ativos na visão {cenario['visao']['epoca']}", lambda: all(
            estado(i)["estado"] == "ativo" and estado(i)["visao"] == cenario["visao"] for i in vivos))
        print(f"nó {cai} derrubado; visão {cenario['visao']} ativa em {t:.1f}s")
        # O coordenador encerra a transição quando os heartbeats mostram todos ativos.
        esperar("transição encerrada no coordenador", lambda: all(
            estado(i)["fase"] is None and estado(i)["transicao"] is None for i in vivos), limite=10)
        assert all(estado(i)["suspeitos"] == [] for i in vivos)
        assert historico(vivos[0]) == historico(vivos[1]) == antes, "histórico alterado na reorganização"
        print(f"históricos dos sobreviventes iguais e preservados ({len(antes)} mensagens)")

        nova, recebido = asyncio.run(mensagem_nova(vivos))
        assert all(ids == [nova] for ids in recebido.values()), recebido
        esperar("mensagem nova nos dois históricos",
                lambda: historico(vivos[0]) == historico(vivos[1]) == antes + [nova])
        print(f"mensagem nova entregue aos clientes dos nós {vivos}; históricos com {len(antes) + 1} mensagens")
    finally:
        compose("down")
        print("docker compose down")


if __name__ == "__main__":
    nomes = sys.argv[1:] or list(CENARIOS)
    for nome in nomes:
        executar(nome)
    print(f"\nvalidação concluída: {', '.join(nomes)}")
