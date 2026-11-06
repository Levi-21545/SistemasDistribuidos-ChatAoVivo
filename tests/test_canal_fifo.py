"""CanalFIFO real com HTTP controlado (httpx.MockTransport), sem rede.

Complementa a simulação: aqui roda o código de retry e bloqueio do transporte.
"""

import asyncio
import copy
import json
import uuid

import httpx

from app.config import Config
from app.transporte import CanalFIFO
from tests.simulacao import criar_grupo, mensagem

CFG = Config(no_id=1, nos={1: "http://no1", 2: "http://no2", 3: "http://no3"},
             retry_inicial=0.001, retry_maximo=0.005)


def pacote(n: int) -> dict:
    return {"tipo": "MSG", "pacote_id": f"1-teste-{n}", "epoca": [1, 3], "origem": 1, "relogio": n,
            "mensagem": mensagem(str(uuid.uuid4()), f"m{n}")}


class Receptor:
    """Responde conforme um roteiro por host; depois do roteiro, 200.

    Itens do roteiro: status HTTP, ou "conexao", "timeout" e "perdida"
    (o pedido chega, mas a resposta se perde).
    """

    def __init__(self, roteiros: dict[str, list]):
        self.roteiros = roteiros
        self.recebidos: list[tuple[str, str]] = []  # (host, pacote_id), na ordem de chegada

    def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        self.recebidos.append((host, json.loads(request.content)["pacote_id"]))
        roteiro = self.roteiros.get(host, [])
        acao = roteiro.pop(0) if roteiro else 200
        if acao == "conexao":
            raise httpx.ConnectError("recusado", request=request)
        if acao == "timeout":
            raise httpx.ReadTimeout("sem resposta", request=request)
        if acao == "perdida":
            raise httpx.ReadError("resposta perdida", request=request)
        return httpx.Response(acao, json={})

    def ids(self, host: str) -> list[str]:
        return [ident for h, ident in self.recebidos if h == host]


async def esperar(condicao, limite: float = 2.0) -> None:
    async with asyncio.timeout(limite):
        while not condicao():
            await asyncio.sleep(0.002)


async def com_canais(transporte, destinos, corpo) -> dict[int, CanalFIFO]:
    """Executa `corpo(canais)` com um canal por destino e encerra tudo no fim."""
    cliente = httpx.AsyncClient(transport=transporte)
    canais = {d: CanalFIFO(d, CFG.nos[d], cliente, CFG) for d in destinos}
    for canal in canais.values():
        canal.iniciar()
    try:
        await corpo(canais)
    finally:
        for canal in canais.values():
            canal.parar()
        await asyncio.gather(*(c._tarefa for c in canais.values()), return_exceptions=True)
        await cliente.aclose()
    return canais


def enviar_e_conferir(roteiro: list, esperados: int) -> tuple[Receptor, CanalFIFO, list[dict]]:
    receptor = Receptor({"no2": roteiro})
    pacotes = [pacote(1), pacote(2)]

    async def corpo(canais):
        for p in pacotes:
            canais[2].enfileirar(p)
        await esperar(lambda: len(receptor.recebidos) == esperados)
        await asyncio.sleep(0.03)  # nada além do esperado chega depois

    canais = asyncio.run(com_canais(httpx.MockTransport(receptor), [2], corpo))
    return receptor, canais[2], pacotes


def test_500_seguido_de_sucesso_repete_o_mesmo_pacote():
    receptor, canal, (p1, p2) = enviar_e_conferir([500, 500], esperados=4)
    assert receptor.ids("no2") == [p1["pacote_id"]] * 3 + [p2["pacote_id"]]
    assert canal.bloqueio is None and canal.tamanho == 0


def test_503_repete_sem_avancar():
    receptor, canal, (p1, p2) = enviar_e_conferir([503, 503, 503], esperados=5)
    assert receptor.ids("no2") == [p1["pacote_id"]] * 4 + [p2["pacote_id"]]
    assert canal.bloqueio is None


def test_falha_de_conexao_e_timeout_repetem_o_mesmo_pacote():
    receptor, canal, (p1, p2) = enviar_e_conferir(["conexao", "timeout"], esperados=4)
    assert receptor.ids("no2") == [p1["pacote_id"]] * 3 + [p2["pacote_id"]]
    assert canal.bloqueio is None


def test_rejeicao_permanente_para_o_canal_sem_ultrapassagem():
    receptor, canal, (p1, p2) = enviar_e_conferir([400], esperados=1)
    assert receptor.ids("no2") == [p1["pacote_id"]]  # sem retry e sem enviar o próximo
    assert canal.bloqueio["pacote"] == p1
    assert "HTTP 400" in canal.bloqueio["motivo"]
    assert canal.tamanho == 1  # o segundo pacote continua na fila, atrás do bloqueado


def test_resposta_perdida_e_reenviada_sem_repetir_efeitos():
    """O receptor é um nó real: o retry com o mesmo pacote_id volta como duplicado."""
    rede = criar_grupo()
    no2 = rede.nos[2]
    for texto in ("a", "b"):
        rede.nos[1].aceitar_do_cliente(mensagem(str(uuid.uuid4()), texto))
    pacotes = list(rede.canais[(1, 2)])  # MSGs reais que o nó 1 emitiu para o nó 2
    acks_antes = len(rede.canais[(2, 1)])
    respostas = []
    perdas = [True]

    def receptor(request):
        status, corpo = no2.receber_pacote(json.loads(request.content))
        respostas.append((json.loads(request.content)["pacote_id"], corpo))
        if perdas and perdas.pop():
            raise httpx.ReadError("resposta perdida", request=request)
        return httpx.Response(status, json=corpo)

    async def corpo(canais):
        for p in pacotes:
            canais[2].enfileirar(copy.deepcopy(p))
        await esperar(lambda: len(respostas) == 3)

    asyncio.run(com_canais(httpx.MockTransport(receptor), [2], corpo))
    p1, p2 = (p["pacote_id"] for p in pacotes)
    assert respostas == [(p1, {"ok": True}), (p1, {"duplicado": True}), (p2, {"ok": True})]
    assert [e.mensagem["texto"] for e in no2.fila.pendentes()] == ["a", "b"]
    assert len(rede.canais[(2, 1)]) - acks_antes == 2  # um ACK por MSG, nenhum pelo retry


def test_destino_indisponivel_nao_atrasa_os_outros():
    receptor = Receptor({"no3": ["conexao"] * 1000})
    p1, p2, p3 = pacote(1), pacote(2), pacote(3)

    async def corpo(canais):
        for p in (p1, p2, p3):
            canais[2].enfileirar(p)
            canais[3].enfileirar(p)
        await esperar(lambda: len(receptor.ids("no2")) == 3 and len(receptor.ids("no3")) >= 5)

    canais = asyncio.run(com_canais(httpx.MockTransport(receptor), [2, 3], corpo))
    assert receptor.ids("no2") == [p1["pacote_id"], p2["pacote_id"], p3["pacote_id"]]
    assert set(receptor.ids("no3")) == {p1["pacote_id"]}  # só retries do primeiro
    assert canais[3].tamanho == 2


def test_canal_bloqueado_nao_para_os_outros_destinos():
    receptor = Receptor({"no3": [400]})
    p1, p2 = pacote(1), pacote(2)

    async def corpo(canais):
        for p in (p1, p2):
            canais[2].enfileirar(p)
            canais[3].enfileirar(p)
        await esperar(lambda: len(receptor.ids("no2")) == 2 and canais[3].bloqueio is not None)
        await asyncio.sleep(0.03)

    canais = asyncio.run(com_canais(httpx.MockTransport(receptor), [2, 3], corpo))
    assert receptor.ids("no2") == [p1["pacote_id"], p2["pacote_id"]]
    assert receptor.ids("no3") == [p1["pacote_id"]]
    assert canais[2].bloqueio is None and canais[3].bloqueio["pacote"] == p1
