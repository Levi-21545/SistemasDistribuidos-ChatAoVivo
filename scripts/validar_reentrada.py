"""Cliente da validação T06, executado pelo shell em um contêiner do Compose.

Reutiliza os clientes HTTP/WebSocket da T05; o shell controla kill/start no host.
O volume efêmero guarda a referência entre as três fases de cada cenário.
"""

import asyncio
import json
import sys
from pathlib import Path

import httpx

import validar_compose as v

v.URLS = {i: f"http://no{i}:8000" for i in v.PORTAS}
REFERENCIA = Path("/evidencias/reentrada.json")


def eventos(i):
    return httpx.get(v.URLS[i] + "/historico", timeout=2).json()


def ativos(membros, epoca):
    return all(v.estado(i)["estado"] == "ativo" and v.estado(i)["visao"] == {
        "epoca": epoca, "lider": max(membros), "membros": sorted(membros)}
        and v.estado(i)["transicao"] is None for i in membros)


async def depois_do_retorno(caido, repetida):
    conexoes = await v.conectar(v.PORTAS)
    try:
        ws = conexoes[caido]
        await ws.send(json.dumps(repetida))
        resposta = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert resposta == {"tipo": "confirmada", "id": repetida["id"]}, resposta
        nova = v.msg("originada pelo nó readmitido")
        await ws.send(json.dumps(nova))
        for conexao in conexoes.values():
            assert await v.entregas(conexao, 1) == [nova["id"]]
        return nova["id"]
    finally:
        for ws in conexoes.values():
            await ws.close()


def executar(fase, caido):
    vivos = sorted(set(v.PORTAS) - {caido})
    if fase == "antes":
        v.esperar("bootstrap", lambda: ativos([1, 2, 3], [1, 3]))
        recebido = asyncio.run(v.concorrentes())
        assert recebido[1] == recebido[2] == recebido[3]
        assert len(set(recebido[1])) == 45
        v.esperar("históricos iniciais iguais", lambda: eventos(1) == eventos(2) == eventos(3))
        REFERENCIA.write_text(json.dumps({"historico": eventos(1)}), encoding="utf-8")
        print(f"nó {caido}: 45 mensagens concorrentes iguais antes da queda", flush=True)
    elif fase == "queda":
        v.esperar("visão reduzida", lambda: ativos(vivos, [2, max(vivos)]))
        referencia = json.loads(REFERENCIA.read_text(encoding="utf-8"))
        assert all(eventos(i) == referencia["historico"] for i in vivos)
        # Guarda o conteúdo da tentativa para testar seu reenvio ao nó reiniciado.
        repetida = v.msg("enviada enquanto o nó estava fora")

        async def enviar():
            conexoes = await v.conectar(vivos)
            try:
                await conexoes[vivos[0]].send(json.dumps(repetida))
                for ws in conexoes.values():
                    assert await v.entregas(ws, 1) == [repetida["id"]]
            finally:
                for ws in conexoes.values():
                    await ws.close()

        asyncio.run(enviar())
        v.esperar("histórico após a queda", lambda: eventos(vivos[0]) == eventos(vivos[1]))
        REFERENCIA.write_text(json.dumps({"historico": eventos(vivos[0]), "mensagem": repetida}), encoding="utf-8")
        print(f"nó {caido}: sobreviventes retomaram e entregaram durante a ausência", flush=True)
    elif fase == "retorno":
        t = v.esperar("readmissão dos três nós", lambda: ativos([1, 2, 3], [3, max(vivos)]))
        referencia = json.loads(REFERENCIA.read_text(encoding="utf-8"))
        assert all(eventos(i) == referencia["historico"] for i in v.PORTAS)
        ident = asyncio.run(depois_do_retorno(caido, referencia["mensagem"]))
        esperado = [e["mensagem"]["id"] for e in referencia["historico"]] + [ident]
        v.esperar("nova mensagem entregue nos três nós", lambda: all(v.historico(i) == esperado for i in v.PORTAS))
        recebido = asyncio.run(v.concorrentes())
        assert recebido[1] == recebido[2] == recebido[3] and len(set(recebido[1])) == 45
        v.esperar("históricos finais iguais", lambda: eventos(1) == eventos(2) == eventos(3))
        assert v.historico(1) == esperado + recebido[1]
        print(f"nó {caido}: readmitido em {t:.1f}s; UUID confirmado sem duplicar; "
              "novo envio e 45 mensagens concorrentes iguais nos três nós", flush=True)
    else:
        raise ValueError(f"fase desconhecida: {fase}")


if __name__ == "__main__":
    executar(sys.argv[1], int(sys.argv[2]))
