"""Integração com três processos uvicorn reais em localhost (HTTP + WebSocket).

Equivale ao Compose, mas com portas locais no lugar do DNS. Os testes rodam
em sequência e o último derruba o líder.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest
from websockets.asyncio.client import connect

pytestmark = pytest.mark.integracao

RAIZ = Path(__file__).resolve().parent.parent
PORTAS = {1: 18001, 2: 18002, 3: 18003}
URLS = {i: f"http://127.0.0.1:{p}" for i, p in PORTAS.items()}
TEMPOS = {"HEARTBEAT_INTERVALO": "0.3", "SUSPEITA_TIMEOUT": "1.2", "ELEICAO_TIMEOUT": "0.5",
          "COORDENADOR_TIMEOUT": "1.5"}


def esperar(condicao, limite=15.0, passo=0.1):
    fim = time.monotonic() + limite
    while time.monotonic() < fim:
        try:
            if condicao():
                return
        except httpx.HTTPError:
            pass
        time.sleep(passo)
    raise AssertionError("condição não atingida a tempo")


def estado(i):
    return httpx.get(URLS[i] + "/estado", timeout=2).json()


def historico(i):
    return [e["mensagem"]["id"] for e in httpx.get(URLS[i] + "/historico", timeout=2).json()]


@pytest.fixture(scope="module")
def grupo(tmp_path_factory):
    logs = tmp_path_factory.mktemp("logs")
    nos = ",".join(f"{i}={u}" for i, u in URLS.items())
    processos = {}
    for i, porta in PORTAS.items():
        env = {**os.environ, **TEMPOS, "NO_ID": str(i), "NOS": nos, "PYTHONIOENCODING": "utf-8"}
        saida = open(logs / f"no{i}.log", "w", encoding="utf-8")
        processos[i] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(porta)],
            cwd=RAIZ, env=env, stdout=saida, stderr=subprocess.STDOUT,
        )
    yield processos
    for p in processos.values():
        p.kill()
        p.wait()
    print(f"\nlogs dos nós em {logs}")


async def cliente(i):
    ws = await connect(f"ws://127.0.0.1:{PORTAS[i]}/ws")
    inicial = json.loads(await ws.recv())
    assert inicial["tipo"] == "historico"
    return ws


async def coletar_entregas(ws, quantidade, limite=20.0):
    ids = []
    async with asyncio.timeout(limite):
        while len(ids) < quantidade:
            dados = json.loads(await ws.recv())
            if dados["tipo"] == "entregue":
                ids.append(dados["mensagem"]["id"])
            elif dados["tipo"] == "recusada":
                raise AssertionError(dados)
    return ids


def msg(texto, autor):
    return {"id": str(uuid.uuid4()), "sala": "geral", "autor": autor, "texto": texto}


def test_bootstrap_ativa_os_tres_nos(grupo):
    esperar(lambda: all(estado(i)["estado"] == "ativo" for i in PORTAS))
    for i in PORTAS:
        assert estado(i)["visao"] == {"epoca": [1, 3], "lider": 3, "membros": [1, 2, 3]}


def test_envios_concorrentes_em_nos_distintos_tem_a_mesma_ordem(grupo):
    por_cliente = 15

    async def cenario():
        conexoes = {i: await cliente(i) for i in PORTAS}
        coletores = {i: asyncio.create_task(coletar_entregas(ws, por_cliente * 3)) for i, ws in conexoes.items()}

        async def disparar(i, ws):
            for k in range(por_cliente):
                await ws.send(json.dumps(msg(f"m{k}", f"cliente{i}")))

        await asyncio.gather(*(disparar(i, ws) for i, ws in conexoes.items()))
        resultado = {i: await t for i, t in coletores.items()}
        for ws in conexoes.values():
            await ws.close()
        return resultado

    sequencias = asyncio.run(cenario())
    assert sequencias[1] == sequencias[2] == sequencias[3]
    assert len(set(sequencias[1])) == por_cliente * 3
    assert historico(1) == historico(2) == historico(3)


def test_mesmo_uuid_enviado_a_dois_nos_aparece_uma_vez(grupo):
    m = msg("repetida", "ana")

    async def cenario():
        ws1, ws2 = await cliente(1), await cliente(2)
        await asyncio.gather(ws1.send(json.dumps(m)), ws2.send(json.dumps(m)))
        await coletar_entregas(ws1, 1)
        await ws1.send(json.dumps(m))  # reenvio após a entrega
        while True:
            dados = json.loads(await asyncio.wait_for(ws1.recv(), 5))
            if dados["tipo"] == "confirmada":
                break
        await ws1.close()
        await ws2.close()

    asyncio.run(cenario())
    esperar(lambda: historico(1) == historico(2) == historico(3))
    assert historico(1).count(m["id"]) == 1


def test_queda_do_lider_reorganiza_e_retoma_com_o_no_2(grupo):
    """T05: os sobreviventes elegem o nó 2, instalam a visão (2,2) e voltam a aceitar mensagens.

    Que a eleição sozinha não libera envios antes da barreira é verificado nos testes
    simulados (test_eleicao e test_reorganizacao), onde a transição pode ser retida.
    """
    antes = historico(1)
    grupo[3].kill()
    grupo[3].wait()

    esperar(lambda: all(estado(i)["estado"] == "ativo" and estado(i)["visao"]["epoca"] == [2, 2]
                        for i in (1, 2)))
    for i in (1, 2):
        e = estado(i)
        assert e["visao"] == {"epoca": [2, 2], "lider": 2, "membros": [1, 2]}
        assert e["coordenador"] == 2 and e["fase"] is None and e["suspeitos"] == []
    assert historico(1) == historico(2)
    assert historico(1)[:len(antes)] == antes

    nova = msg("depois da queda", "ana")

    async def enviar_depois():
        ws1, ws2 = await cliente(1), await cliente(2)
        await ws1.send(json.dumps(nova))
        recebidos = [await coletar_entregas(ws, 1) for ws in (ws1, ws2)]
        await ws1.close()
        await ws2.close()
        return recebidos

    assert asyncio.run(enviar_depois()) == [[nova["id"]], [nova["id"]]]
    esperar(lambda: historico(1) == historico(2) and historico(1)[-1] == nova["id"])
