"""Cliente da validação de retry (T08), executado pelo shell em um contêiner do Compose.

Fases (o shell congela e descongela o nó 1 entre elas com docker pause/unpause):
  antes    espera os três nós ativos e guarda o histórico de referência.
  enviar   com o nó 1 congelado, envia mensagens pelo cliente do nó 2 sem esperar
           a entrega: o MSG e o ACK para o nó 1 estouram o tempo e ficam em retry.
  conferir depois do unpause, cada mensagem aparece uma única vez, na mesma ordem
           nos três nós, e a visão continua (1,3): houve retry, não reorganização.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

import httpx

import validar_compose as v

v.URLS = {i: f"http://no{i}:8000" for i in v.PORTAS}
REFERENCIA = Path("/evidencias/retry.json")
QUANTIDADE = 10


def executar(fase):
    if fase == "antes":
        v.esperar("três nós ativos na visão (1,3)", lambda: all(
            v.estado(i)["estado"] == "ativo" and v.estado(i)["visao"]["epoca"] == [1, 3] for i in v.PORTAS))
        REFERENCIA.write_text(json.dumps({"antes": v.historico(1)}), encoding="utf-8")
        print("três nós ativos na visão (1,3)", flush=True)
    elif fase == "enviar":
        mensagens = [v.msg(f"retry-{k}") for k in range(QUANTIDADE)]

        async def enviar():
            conexoes = await v.conectar([2])
            ws = conexoes[2]
            print("cliente conectado ao nó 2; aguardando o nó 1 congelar", flush=True)
            async with httpx.AsyncClient() as http:
                while True:  # o shell congela o nó 1 a qualquer momento
                    try:
                        await http.get(v.URLS[1] + "/estado", timeout=0.3)
                        await asyncio.sleep(0.05)
                    except httpx.HTTPError:
                        break
            for m in mensagens:
                await ws.send(json.dumps(m))
                resposta = json.loads(await asyncio.wait_for(ws.recv(), 5))
                assert resposta == {"tipo": "aceita", "id": m["id"]}, resposta
            await ws.close()

        asyncio.run(enviar())
        time.sleep(0.3)
        assert len(v.historico(2)) == len(json.loads(REFERENCIA.read_text())["antes"]), \
            "nó 2 entregou sem o ACK do nó 1 congelado"
        dados = json.loads(REFERENCIA.read_text(encoding="utf-8"))
        dados["enviadas"] = [m["id"] for m in mensagens]
        REFERENCIA.write_text(json.dumps(dados), encoding="utf-8")
        print(f"{QUANTIDADE} mensagens aceitas pelo nó 2 com o nó 1 congelado; nenhuma entregue ainda", flush=True)
    elif fase == "conferir":
        dados = json.loads(REFERENCIA.read_text(encoding="utf-8"))
        esperado = dados["antes"] + dados["enviadas"]
        t = v.esperar("mensagens entregues nos três nós",
                      lambda: all(v.historico(i) == esperado for i in v.PORTAS), limite=20)
        for i in v.PORTAS:
            e = v.estado(i)
            assert e["estado"] == "ativo" and e["visao"]["epoca"] == [1, 3] and e["suspeitos"] == [], e
        print(f"após o unpause, as {QUANTIDADE} mensagens chegaram em {t:.1f}s, uma vez cada e na mesma ordem "
              "nos três nós; visão (1,3) mantida, sem suspeitos", flush=True)
    else:
        raise ValueError(f"fase desconhecida: {fase}")


if __name__ == "__main__":
    try:
        executar(sys.argv[1])
    except httpx.HTTPError as erro:
        raise SystemExit(f"falha HTTP: {erro!r}")
