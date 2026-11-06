"""T05: reorganização após uma queda, sobre a rede simulada (FIFO por canal).

O tempo dos nós é `rede.tempo`; cada rodada de `ate()` avança o relógio, roda o
ciclo de heartbeat dos vivos e entrega controles e dados. A eleição usa o Bully
real, com timeouts curtos do asyncio.
"""

import asyncio
import copy
import random
import uuid

import pytest

from app.config import Config
from app.modelos import Visao
from app.no import ATIVO, CORTE, INSTALADA, REORGANIZANDO, SINCRONIZANDO, No
from tests.simulacao import RedeSimulada, criar_grupo, ids_entregues, mensagem

TEMPOS = dict(heartbeat_intervalo=0.05, suspeita_timeout=0.5, eleicao_timeout=0.01, coordenador_timeout=0.05)


def grupo(seed=0):
    return criar_grupo(seed=seed, **TEMPOS)


def nova(texto):
    return mensagem(str(uuid.uuid4()), texto)


def textos(no):
    return [e.mensagem["texto"] for e in no.historico]


async def ate(rede, condicao, limite=5.0, dt=0.1):
    """Avança o tempo em rodadas de heartbeat até a condição valer."""
    fim = asyncio.get_running_loop().time() + limite
    while not condicao():
        assert asyncio.get_running_loop().time() < fim, "condição não atingida a tempo"
        rede.tempo += dt
        for i, no in rede.nos.items():
            if i not in rede.mortos:
                no.ciclo()
        rede.drenar()
        await asyncio.sleep(0.02)  # vez das eleições, que esperam timeouts reais


def retomados(rede, epoca, membros):
    return lambda: all(rede.nos[i].estado == ATIVO and rede.nos[i].visao.epoca == epoca for i in membros)


class Cliente:
    """Como o WebSocket: recebe o histórico publicado ao conectar e depois cada entrega."""

    def __init__(self, no):
        self.recebido = [(i, e.uuid) for i, e in enumerate(no.historico, start=1)]
        no.ouvintes.append(lambda evento, posicao: self.recebido.append((posicao, evento.uuid)))

    def conferir(self, no):
        posicoes = [p for p, _ in self.recebido]
        assert posicoes == list(range(1, len(posicoes) + 1)), "posição repetida ou fora de ordem"
        assert [u for _, u in self.recebido] == ids_entregues(no), "sufixo duplicado ou ausente"


def enviar_concorrentes(rede, quantidade, seed):
    """Clientes enviam a nós ativos aleatórios enquanto a rede entrega pacotes."""
    rng = rede.rng
    enviados = 0
    while enviados < quantidade:
        if rng.random() < 0.4:
            ativos = [i for i, n in rede.nos.items() if n.estado == ATIVO and i not in rede.mortos]
            assert rede.nos[rng.choice(ativos)].aceitar_do_cliente(nova(f"s{seed}-{enviados}"))["tipo"] == "aceita"
            enviados += 1
        else:
            rede.passo()


# --------------------------------------------------------------------- quedas

@pytest.mark.parametrize("seed", range(15))
def test_queda_do_lider_no_meio_do_trafego(seed):
    async def cenario():
        rede = grupo(seed)
        enviar_concorrentes(rede, 12, seed)
        antes = {i: ids_entregues(rede.nos[i]) for i in (1, 2)}
        rede.mortos.add(3)  # cai com pacotes ainda nos canais
        await ate(rede, retomados(rede, (2, 2), (1, 2)))

        for i in (1, 2):
            no = rede.nos[i]
            assert no.visao == Visao((2, 2), 2, frozenset({1, 2}))
            assert ids_entregues(no)[:len(antes[i])] == antes[i]  # prefixo confirmado preservado
        assert ids_entregues(rede.nos[1]) == ids_entregues(rede.nos[2])

        depois = [nova("depois-1"), nova("depois-2")]
        assert rede.nos[1].aceitar_do_cliente(depois[0])["tipo"] == "aceita"
        assert rede.nos[2].aceitar_do_cliente(depois[1])["tipo"] == "aceita"
        rede.drenar()
        assert ids_entregues(rede.nos[1]) == ids_entregues(rede.nos[2])
        assert {m["id"] for m in depois} <= set(ids_entregues(rede.nos[1]))

    asyncio.run(cenario())


@pytest.mark.parametrize("caido", [1, 2])
def test_queda_de_membro_comum_preserva_o_lider(caido):
    async def cenario():
        rede = grupo(caido)
        enviar_concorrentes(rede, 8, caido)
        rede.mortos.add(caido)
        vivos = tuple(sorted({1, 2, 3} - {caido}))
        await ate(rede, retomados(rede, (2, 3), vivos))
        for i in vivos:
            assert rede.nos[i].visao == Visao((2, 3), 3, frozenset(vivos))
            assert rede.nos[i].coordenador == 3
        assert ids_entregues(rede.nos[vivos[0]]) == ids_entregues(rede.nos[vivos[1]])
        assert rede.nos[vivos[0]].aceitar_do_cliente(nova("depois"))["tipo"] == "aceita"
        rede.drenar()
        assert textos(rede.nos[vivos[0]])[-1] == textos(rede.nos[vivos[1]])[-1] == "depois"

    asyncio.run(cenario())


def test_percepcao_em_momentos_diferentes():
    """Só o nó 2 percebe a queda do líder; o PREPARE chega ao nó 1 antes da suspeita dele."""
    async def cenario():
        rede = grupo()
        rede.mortos.add(3)
        rede.nos[2]._suspeitar(3)
        await ate(rede, retomados(rede, (2, 2), (1, 2)), dt=0.01)  # dt curto: o nó 1 não chega a suspeitar
        assert rede.nos[1].visao.membros == {1, 2}

    asyncio.run(cenario())


# ------------------------------------------------------------ exemplo do protocolo

def montar_exemplo(rede):
    """Visão (1,3): nó 1 com [oi, x] e y pendente; nó 2 com [oi] e x pendente; o nó 3 cai.

    Sequência FIFO: o ACK do nó 3 para x e a cópia de y para o nó 2 ficam, nessa
    ordem, em trânsito no canal 3->2. O nó 2 tem o MSG de x, mas não o ACK do 3.
    """
    oi = nova("oi")
    rede.nos[1].aceitar_do_cliente(oi)
    rede.drenar()
    assert all(textos(n) == ["oi"] for n in rede.nos.values())

    assert rede.nos[1].relogio.valor < 9
    rede.nos[1].relogio.valor = 9  # eventos locais do nó 1
    x = nova("x")
    rede.nos[1].aceitar_do_cliente(x)  # carimbo (10, 1)
    rede.entregar(1, 3)  # MSG x -> 3; ACK de 3 vai para (3,1) e (3,2)
    rede.entregar(1, 2)  # MSG x -> 2; ACK de 2 vai para (2,1) e (2,3)
    rede.entregar(2, 1)  # ACK de 2 -> 1
    rede.entregar(3, 1)  # ACK de 3 -> 1: o nó 1 entrega x
    rede.nos[3].relogio.valor = 19  # eventos locais do nó 3
    y = nova("y")
    rede.nos[3].aceitar_do_cliente(y)  # carimbo (20, 3), atrás do ACK de x no canal (3,2)
    rede.entregar(3, 1)  # MSG y -> 1: pendente, falta o ACK do nó 2
    rede.mortos.add(3)
    rede.segurados.add((3, 2))  # ACK de x e cópia de y seguem em trânsito para o nó 2
    return oi, x, y


def test_exemplo_mensagens_em_transito():
    async def cenario():
        rede = grupo()
        oi, x, y = montar_exemplo(rede)
        no1, no2 = rede.nos[1], rede.nos[2]
        assert textos(no1) == ["oi", "x"] and [e.mensagem["texto"] for e in no1.fila.pendentes()] == ["y"]
        assert textos(no2) == ["oi"] and [e.mensagem["texto"] for e in no2.fila.pendentes()] == ["x"]
        assert [e.ordem for e in no1.historico[1:] + no1.fila.pendentes()] == [(10, 1), (20, 3)]
        clientes = {1: Cliente(no1), 2: Cliente(no2)}

        # O VIEW_STATE do nó 1 se perde: o coordenador fica na coleta depois dos dois cortes.
        rede.perder = lambda o, d, p: p["tipo"] == "VIEW_STATE"
        await ate(rede, lambda: no1.fase == CORTE and no2.fase == CORTE)
        assert no2.coordenador == 2 and no2._transicao.epoca == (2, 2)
        captura = copy.deepcopy(no2._corte["captura"])

        rede.segurados.clear()
        rede.respostas.clear()
        rede.entregar(3, 2)  # ACK antigo de x
        rede.entregar(3, 2)  # cópia antiga de y
        assert [(s, c) for _, _, s, c in rede.respostas] == [(200, {"descartado": "visão encerrada"})] * 2
        assert no2._corte["captura"] == captura and textos(no2) == ["oi"]

        rede.perder = None  # o coordenador reenvia o PREPARE e o nó 1 devolve a mesma captura
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        for no in (no1, no2):
            assert no.visao == Visao((2, 2), 2, frozenset({1, 2}))
            assert textos(no) == ["oi", "x", "y"]
        assert clientes[1].recebido[2:] == [(3, y["id"])]  # nó 1 publica só y
        assert clientes[2].recebido[1:] == [(2, x["id"]), (3, y["id"])]  # nó 2 publica x e y
        for i, cliente in clientes.items():
            cliente.conferir(rede.nos[i])

    asyncio.run(cenario())


# ------------------------------------------------------------ controles

def test_controles_perdidos_e_repetidos():
    async def cenario():
        rede = grupo()
        montar_exemplo(rede)
        rede.segurados.clear()
        perdidos = set()

        def perder_primeiro(origem, destino, pacote):  # cada controle da transição se perde uma vez
            if pacote["tipo"].startswith("VIEW_") and pacote["tipo"] not in perdidos:
                perdidos.add(pacote["tipo"])
                return True
            return False

        rede.perder = perder_primeiro
        clientes = {i: Cliente(rede.nos[i]) for i in (1, 2)}
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        assert perdidos == {"VIEW_PREPARE", "VIEW_STATE", "VIEW_INSTALL", "VIEW_READY", "VIEW_RESUME"}
        await ate(rede, lambda: rede.nos[2]._transicao is None)  # coordenador viu todos ativos

        # Reenviar todos os controles já entregues (PREPARE, STATE, INSTALL, READY, RESUME atrasados).
        fotos = {i: (rede.nos[i].estado, rede.nos[i].visao, ids_entregues(rede.nos[i]), rede.nos[i].fase,
                     rede.nos[i].relogio.valor) for i in (1, 2)}
        avisos = {i: list(c.recebido) for i, c in clientes.items()}
        repetidos = [c for c in rede.controles if c[2]["tipo"].startswith("VIEW_")]
        assert {c[2]["tipo"] for c in repetidos} >= {"VIEW_PREPARE", "VIEW_STATE", "VIEW_RESUME"}
        for _, destino, pacote in repetidos:
            status, corpo = rede.nos[destino].receber_pacote(copy.deepcopy(pacote))
            assert status == 200 and "ok" not in corpo, (pacote["tipo"], corpo)
        rede.drenar()
        for i in (1, 2):
            no = rede.nos[i]
            assert (no.estado, no.visao, ids_entregues(no), no.fase, no.relogio.valor) == fotos[i]
            assert clientes[i].recebido == avisos[i]  # nenhuma notificação repetida
            clientes[i].conferir(no)

    asyncio.run(cenario())


# ------------------------------------------------------------ dados e transporte

def test_dados_antigos_encerrados_e_novos_aguardam_o_resume():
    async def cenario():
        rede = grupo()
        montar_exemplo(rede)
        no1, no2 = rede.nos[1], rede.nos[2]
        assert any(p["tipo"] == "ACK" for p in rede.canais[(1, 3)])  # ACK de y para o nó que caiu
        rede.perder = lambda o, d, p: p["tipo"] == "VIEW_RESUME" and d == 1
        await ate(rede, lambda: no2.estado == ATIVO and no1.fase == INSTALADA)

        # Os cortes encerraram as filas antigas de 1 e 2, inclusive para o nó removido.
        for (origem, destino), fila in rede.canais.items():
            if origem in (1, 2):
                assert all(tuple(p["epoca"]) > (1, 3) for p in fila), (origem, destino)

        # Dados da nova visão chegam ao nó 1 antes do RESUME: 503, sem processar.
        rede.respostas.clear()
        novo = nova("nova época")
        assert no2.aceitar_do_cliente(novo)["tipo"] == "aceita"
        rede.drenar(limite=20)
        assert {(d, s) for d, p, s, _ in rede.respostas if p["tipo"] == "MSG"} == {(1, 503)}
        assert novo["id"] not in ids_entregues(no1) and no1.fila.pendentes() == []

        rede.perder = None  # o RESUME é reenviado; o MSG passa no retry seguinte
        await ate(rede, lambda: textos(no1)[-1:] == textos(no2)[-1:] == ["nova época"])
        assert ids_entregues(no1) == ids_entregues(no2)

    asyncio.run(cenario())


def test_eleicao_sozinha_nao_retoma():
    async def cenario():
        rede = grupo()
        rede.mortos.add(3)
        rede.perder = lambda o, d, p: p["tipo"] == "VIEW_PREPARE"  # nenhum participante corta
        await ate(rede, lambda: rede.nos[1].coordenador == 2 and rede.nos[2].coordenador == 2)
        for _ in range(5):
            await ate(rede, lambda: True)
            rede.tempo += 0.5
            for i in (1, 2):
                rede.nos[i].ciclo()
            rede.drenar()
        assert rede.nos[1].estado == REORGANIZANDO and rede.nos[1].fase is None
        assert rede.nos[2].fase == CORTE  # o coordenador aguarda a coleta
        assert rede.nos[1].aceitar_do_cliente(nova("cedo"))["tipo"] == "recusada"
        assert rede.nos[2].visao.epoca == (1, 3)

    asyncio.run(cenario())


# ------------------------------------------------------------ incompatibilidade e UUID

def test_historicos_incompativeis_mantem_pausa_com_erro():
    async def cenario():
        rede = grupo()
        for texto in ("a", "b"):
            rede.nos[1].aceitar_do_cliente(nova(texto))
            rede.drenar()
        # Estado impossível, só para exercitar a defesa: o nó 1 publicou outro conteúdo em "b".
        alterado = rede.nos[1].historico[1]
        rede.nos[1].historico[1] = type(alterado)(alterado.epoca, alterado.relogio, alterado.origem,
                                                  {**alterado.mensagem, "texto": "b'"})
        antes = {i: [e.json() for e in rede.nos[i].historico] for i in (1, 2)}
        rede.mortos.add(3)
        await ate(rede, lambda: rede.nos[2].erro_transicao is not None)
        for _ in range(3):
            await ate(rede, lambda: True)
            rede.tempo += 0.5
            for i in (1, 2):
                rede.nos[i].ciclo()
            rede.drenar()

        assert "históricos incompatíveis" in rede.nos[2].erro_transicao
        assert rede.nos[2].estado_publico()["transicao"]["erro"] == rede.nos[2].erro_transicao
        for i in (1, 2):
            no = rede.nos[i]
            assert no.estado == REORGANIZANDO and no.fase == CORTE and no.visao.epoca == (1, 3)
            assert [e.json() for e in no.historico] == antes[i]
            assert no.aceitar_do_cliente(nova("durante o erro"))["tipo"] == "recusada"

    asyncio.run(cenario())


def test_mesmo_uuid_em_nos_distintos_mantem_a_primeira_ocorrencia():
    async def cenario():
        rede = grupo()
        m = nova("repetida")
        rede.nos[1].aceitar_do_cliente(m)  # primeira ocorrência, carimbo menor
        rede.nos[2].relogio.valor += 5
        rede.nos[2].aceitar_do_cliente(m)  # o cliente reenviou ao nó 2 antes da entrega
        rede.entregar(1, 3)
        rede.entregar(1, 2)
        rede.entregar(3, 2)  # nó 2 entrega a primeira ocorrência
        rede.entregar(3, 1)
        rede.entregar(2, 1)  # MSG da segunda ocorrência -> 1
        rede.entregar(2, 1)  # ACK de 2 para a primeira: nó 1 entrega a primeira ocorrência
        rede.mortos.add(3)
        for i in (1, 2):
            assert ids_entregues(rede.nos[i]) == [m["id"]]
            assert [e.uuid for e in rede.nos[i].fila.pendentes()] == [m["id"]]  # a segunda segue pendente
        confirmada = {i: rede.nos[i].historico[0].json() for i in (1, 2)}
        clientes = {i: Cliente(rede.nos[i]) for i in (1, 2)}

        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        for i in (1, 2):
            no = rede.nos[i]
            assert ids_entregues(no) == [m["id"]] and no.historico[0].json() == confirmada[i]
            assert clientes[i].recebido == [(1, m["id"])]  # nada republicado
            assert no.aceitar_do_cliente(m) == {"tipo": "confirmada", "id": m["id"]}

    asyncio.run(cenario())


# ------------------------------------------------------------ relógio, maioria e reinício

def test_relogio_nao_diminui_na_instalacao():
    async def cenario():
        rede = grupo()
        montar_exemplo(rede)
        rede.segurados.clear()
        rede.nos[1].relogio.valor = 50  # nó 1 adiantado em relação ao coordenador
        no2_antes = rede.nos[2].relogio.valor
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        assert rede.nos[1].relogio.valor >= 50
        assert rede.nos[2].relogio.valor >= 50 > no2_antes  # referência = maior relógio informado

    asyncio.run(cenario())


def test_sobrevivente_sozinho_nao_retoma():
    async def cenario():
        rede = grupo()
        rede.mortos.add(3)
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        rede.mortos.add(2)  # segunda queda, já na visão (2,2): sobra um nó de três
        await ate(rede, lambda: rede.nos[1].erro_transicao is not None)
        await ate(rede, lambda: True)
        no1 = rede.nos[1]
        assert "sem maioria" in no1.erro_transicao
        assert no1.estado == REORGANIZANDO and no1.visao.epoca == (2, 2)
        assert no1.aceitar_do_cliente(nova("sozinho"))["tipo"] == "recusada"

    asyncio.run(cenario())


def reiniciar(rede, i):
    """Novo processo: nova encarnação, sem visão nem histórico."""
    cfg = Config(no_id=i, nos={j: f"http://no{j}" for j in rede.nos}, **TEMPOS)
    rede.nos[i] = No(cfg, rede.para(i), agora=lambda: rede.tempo)
    rede.mortos.discard(i)


def test_no_reiniciado_depois_da_reorganizacao_nao_e_admitido():
    async def cenario():
        rede = grupo()
        rede.mortos.add(3)
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        reiniciar(rede, 3)
        for _ in range(10):
            await ate(rede, lambda: True)
            rede.tempo += 0.1
            for no in rede.nos.values():
                no.ciclo()
            rede.drenar()
        assert rede.nos[3].estado == SINCRONIZANDO and rede.nos[3].visao is None
        for i in (1, 2):
            assert rede.nos[i].estado == ATIVO and rede.nos[i].visao.membros == {1, 2}

    asyncio.run(cenario())


def test_no_reiniciado_antes_da_suspeita_conta_como_falha():
    """O nó 3 volta antes do timeout: os heartbeats sem visão denunciam o reinício."""
    async def cenario():
        rede = grupo()
        rede.nos[1].aceitar_do_cliente(nova("antes"))
        rede.drenar()
        await ate(rede, lambda: all(n.estado_pares.get(3, {}).get("epoca") == [1, 3] for n in
                                    (rede.nos[1], rede.nos[2])), dt=0.05)
        reiniciar(rede, 3)
        await ate(rede, retomados(rede, (2, 2), (1, 2)), dt=0.05)
        assert rede.nos[3].estado == SINCRONIZANDO and rede.nos[3].visao is None
        assert textos(rede.nos[1]) == textos(rede.nos[2]) == ["antes"]

    asyncio.run(cenario())


# ------------------------------------------------------------ clientes e barreira

def test_clientes_nao_recebem_o_sufixo_antes_do_resume():
    async def cenario():
        rede = grupo()
        oi, x, y = montar_exemplo(rede)  # o canal 3->2 segue retido: x e y entram pela reconciliação
        no2 = rede.nos[2]
        antes = Cliente(no2)  # conectado desde antes da queda
        rede.perder = lambda o, d, p: p["tipo"] == "VIEW_RESUME"
        await ate(rede, lambda: rede.nos[1].fase == INSTALADA and no2.estado == ATIVO)
        no1 = rede.nos[1]
        assert textos(no1) == ["oi", "x"]  # /historico e o histórico inicial: só o já publicado
        durante = Cliente(no1)  # conecta durante a instalação
        assert durante.recebido == [(1, oi["id"]), (2, x["id"])]
        assert no1.aceitar_do_cliente(y)["tipo"] == "recusada"  # sem confirmação antecipada

        rede.perder = None
        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        assert durante.recebido[2:] == [(3, y["id"])]
        assert no1.aceitar_do_cliente(y) == {"tipo": "confirmada", "id": y["id"]}
        durante.conferir(no1)
        antes.conferir(no2)
        assert antes.recebido == [(1, oi["id"]), (2, x["id"]), (3, y["id"])]

    asyncio.run(cenario())


def test_estado_publico_mostra_fase_e_aguardados():
    async def cenario():
        rede = grupo()
        rede.mortos.add(3)
        rede.perder = lambda o, d, p: p["tipo"] == "VIEW_STATE"
        await ate(rede, lambda: rede.nos[1].fase == CORTE)
        coord, part = rede.nos[2].estado_publico(), rede.nos[1].estado_publico()
        assert coord["fase"] == CORTE and coord["transicao"]["etapa"] == "coleta"
        assert coord["transicao"]["aguardando"] == [1] and coord["transicao"]["anterior"] == [1, 3]
        assert part["transicao"] == {"papel": "participante", "epoca": [2, 2], "anterior": [1, 3],
                                     "membros": [1, 2], "etapa": CORTE, "aguardando": [2], "erro": None}

    asyncio.run(cenario())


# ------------------------------------------------------------ regressões da revisão

def test_duplicata_ja_resolvida_nao_bloqueia_a_recuperacao():
    """Nó 1 entregou [A(1,1), B(11,1)] e descartou A(6,2), outra ocorrência do UUID de A.
    Nó 2 entregou [A(1,1)] e tem A(6,2) e B(11,1) pendentes; o ACK do nó 3 para A(6,2)
    está em trânsito no canal 3->2 quando o líder cai.
    """
    async def cenario():
        rede = grupo()
        no1, no2 = rede.nos[1], rede.nos[2]
        a, b = nova("A"), nova("B")
        no1.aceitar_do_cliente(a)                      # A(1,1)
        no2.relogio.valor = 5
        no2.aceitar_do_cliente(a)                      # A(6,2): o cliente reenviou ao nó 2 antes da entrega
        rede.entregar(1, 3)                            # A(1,1) -> 3
        rede.entregar(2, 3)                            # A(6,2) -> 3
        rede.entregar(1, 2)                            # A(1,1) -> 2
        rede.entregar(3, 2)                            # ACK3 A(1,1) -> 2: nó 2 entrega A(1,1)
        rede.entregar(2, 1)                            # A(6,2) -> 1
        rede.entregar(2, 1)                            # ACK2 A(1,1) -> 1
        rede.entregar(3, 1)                            # ACK3 A(1,1) -> 1: nó 1 entrega A(1,1)
        rede.entregar(3, 1)                            # ACK3 A(6,2) -> 1: duplicata descartada na entrega
        no1.relogio.valor = 10
        no1.aceitar_do_cliente(b)                      # B(11,1)
        rede.entregar(1, 3)                            # ACK1 A(6,2) -> 3
        rede.entregar(1, 3)                            # B -> 3
        rede.entregar(1, 2)                            # ACK1 A(6,2) -> 2
        rede.entregar(1, 2)                            # B -> 2
        rede.entregar(2, 1)                            # ACK2 B -> 1
        rede.entregar(3, 1)                            # ACK3 B -> 1: nó 1 entrega B
        rede.mortos.add(3)
        rede.segurados.add((3, 2))                     # ACK3 de A(6,2) e de B seguem em trânsito

        assert textos(no1) == ["A", "B"] and [e.ordem for e in no1.historico] == [(1, 1), (11, 1)]
        assert textos(no2) == ["A"]
        assert [(e.uuid, e.ordem) for e in no2.fila.pendentes()] == [(a["id"], (6, 2)), (b["id"], (11, 1))]
        clientes = {i: Cliente(rede.nos[i]) for i in (1, 2)}

        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        for i in (1, 2):
            assert textos(rede.nos[i]) == ["A", "B"]
            assert [e.ordem for e in rede.nos[i].historico] == [(1, 1), (11, 1)]  # confirmação preservada
            clientes[i].conferir(rede.nos[i])
        assert clientes[1].recebido == [(1, a["id"]), (2, b["id"])]  # nó 1 não republica nada
        assert clientes[2].recebido == [(1, a["id"]), (2, b["id"])]  # nó 2 publica só B

    asyncio.run(cenario())


def grupo_no_bootstrap():
    """Três nós recém-iniciados, sem visão: a visão inicial sai do bootstrap real."""
    rede = RedeSimulada((1, 2, 3), random.Random(0))
    for i in (1, 2, 3):
        cfg = Config(no_id=i, nos={j: f"http://no{j}" for j in (1, 2, 3)}, **TEMPOS)
        rede.nos[i] = No(cfg, rede.para(i), agora=lambda: rede.tempo)
    return rede


def test_reinicio_logo_apos_o_bootstrap_conta_como_falha():
    """O nó 3 reinicia antes que algum heartbeat dele com a época instalada chegue aos outros."""
    async def cenario():
        rede = grupo_no_bootstrap()
        await ate(rede, retomados(rede, (1, 3), (1, 2, 3)))
        for i in (1, 2):
            assert rede.nos[i].estado_pares[3]["epoca"] is None  # só conhecem o 3 sem visão
        reiniciar(rede, 3)

        await ate(rede, retomados(rede, (2, 2), (1, 2)))
        assert rede.nos[3].estado == SINCRONIZANDO and rede.nos[3].visao is None
        nova_msg = nova("depois do reinício")
        assert rede.nos[1].aceitar_do_cliente(nova_msg)["tipo"] == "aceita"
        rede.drenar()
        for i in (1, 2):
            assert ids_entregues(rede.nos[i]) == [nova_msg["id"]]

    asyncio.run(cenario())


def test_heartbeat_atrasado_do_bootstrap_nao_e_reinicio():
    """Um heartbeat antigo, com época nula, chega depois da instalação: mesma encarnação, sem suspeita."""
    async def cenario():
        rede = grupo_no_bootstrap()
        await ate(rede, retomados(rede, (1, 3), (1, 2, 3)))
        await ate(rede, lambda: all(n.estado_pares[3]["epoca"] == [1, 3] for n in (rede.nos[1], rede.nos[2])))
        antigos = [(d, p) for o, d, p in rede.controles
                   if o == 3 and p["tipo"] == "HEARTBEAT" and p["epoca"] is None]
        assert antigos
        for destino, pacote in antigos:
            assert rede.nos[destino].receber_pacote(copy.deepcopy(pacote)) == (200, {"ok": True})
        await ate(rede, lambda: True)
        for i in (1, 2):
            assert rede.nos[i].suspeitos == set() and rede.nos[i].estado == ATIVO
        assert rede.nos[1].aceitar_do_cliente(nova("normal"))["tipo"] == "aceita"
        rede.drenar()
        assert textos(rede.nos[1]) == textos(rede.nos[2]) == textos(rede.nos[3]) == ["normal"]

    asyncio.run(cenario())
