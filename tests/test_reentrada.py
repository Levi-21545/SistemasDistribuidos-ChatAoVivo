"""T06: admissão por JOIN/SNAPSHOT com a barreira de troca de visão da T05."""

import asyncio
import copy

import pytest

from app.no import ATIVO, INSTALADA, SINCRONIZANDO
from tests.simulacao import ids_entregues
from tests.test_reorganizacao import (Cliente, ate, enviar_concorrentes, grupo, nova,
                                     reiniciar, retomados)


async def preparar(caido=3, seed=0):
    rede = grupo(seed)
    # Registra as encarnações antes da queda, como no bootstrap real.
    for no in rede.nos.values():
        no.ciclo()
    rede.drenar()
    enviar_concorrentes(rede, 8, seed)
    rede.drenar()
    rede.mortos.add(caido)
    vivos = sorted(set(rede.nos) - {caido})
    await ate(rede, retomados(rede, (2, max(vivos)), vivos))
    await ate(rede, lambda: all(rede.nos[i]._transicao is None for i in vivos))
    m = nova("enquanto o nó estava fora")
    rede.nos[vivos[0]].aceitar_do_cliente(m)
    rede.drenar()
    return rede, vivos, m


@pytest.mark.parametrize("caido", [1, 2, 3])
@pytest.mark.parametrize("seed", range(6))
def test_reentrada_sincroniza_historico_uuid_relogio_e_clientes(caido, seed):
    async def cenario():
        rede, vivos, m = await preparar(caido, seed)
        prefixo = [e.json() for e in rede.nos[vivos[0]].historico]
        clientes = {i: Cliente(rede.nos[i]) for i in vivos}
        # A admissão também precisa recuperar dados em trânsito entre sobreviventes.
        enviar_concorrentes(rede, 10, seed)
        relogio = max(rede.nos[i].relogio.valor for i in vivos)
        reiniciar(rede, caido)
        clientes[caido] = Cliente(rede.nos[caido])
        assert not rede.nos[caido]._participa_da_eleicao()
        assert rede.nos[caido].aceitar_do_cliente(nova("antes do snapshot"))["tipo"] == "recusada"
        await ate(rede, retomados(rede, (3, max(vivos)), (1, 2, 3)))
        referencia = [e.json() for e in rede.nos[1].historico]
        assert referencia[:len(prefixo)] == prefixo
        assert len(referencia) == len(prefixo) + 10
        for i, no in rede.nos.items():
            assert [e.json() for e in no.historico] == referencia
            assert no.coordenador == no.visao.lider == 3
            assert no.visao.membros == {1, 2, 3}
            assert no.relogio.valor >= relogio
            assert no.aceitar_do_cliente(m)["tipo"] == "confirmada"
            clientes[i].conferir(no)
        alterada = m | {"texto": "conteúdo diferente"}
        assert rede.nos[caido].aceitar_do_cliente(alterada)["tipo"] == "recusada"
        depois = nova("originada pelo nó readmitido")
        assert rede.nos[caido].aceitar_do_cliente(depois)["tipo"] == "aceita"
        rede.drenar()
        assert all(ids_entregues(n) == ids_entregues(rede.nos[1]) for n in rede.nos.values())
        assert all(ids_entregues(n)[-1] == depois["id"] for n in rede.nos.values())
        for i, no in rede.nos.items():
            clientes[i].conferir(no)

    asyncio.run(cenario())


@pytest.mark.parametrize("tipo", ["JOIN", "VIEW_PREPARE", "VIEW_STATE", "SNAPSHOT", "VIEW_READY", "VIEW_RESUME"])
def test_controle_perdido_e_repetido_na_admissao(tipo):
    async def cenario():
        rede, vivos, m = await preparar()
        perdidos = []

        def perder(o, d, p):
            if p["tipo"] == tipo and not perdidos:
                perdidos.append(copy.deepcopy(p))
                return True
            return False

        rede.perder = perder
        inicio = len(rede.controles)
        reiniciar(rede, 3)
        await ate(rede, retomados(rede, (3, 2), (1, 2, 3)))
        assert perdidos
        clientes = {i: Cliente(no) for i, no in rede.nos.items()}
        antes = {i: [e.json() for e in no.historico] for i, no in rede.nos.items()}
        # Repetições após a retomada não reabrem a admissão nem publicam em dobro.
        for _, destino, p in rede.controles[inicio:]:
            assert rede.nos[destino].receber_pacote(copy.deepcopy(p))[0] == 200
        rede.drenar()
        for i, no in rede.nos.items():
            assert no.estado == ATIVO and no.fase is None
            assert [e.json() for e in no.historico] == antes[i]
            clientes[i].conferir(no)

    asyncio.run(cenario())


@pytest.mark.parametrize("bloquear", ["SNAPSHOT", "VIEW_READY", "VIEW_RESUME"])
def test_entrante_nao_publica_nem_participa_antes_da_barreira(bloquear):
    async def cenario():
        rede, vivos, m = await preparar()
        rede.perder = lambda o, d, p: p["tipo"] == bloquear and (d == 3 or o == 3)
        reiniciar(rede, 3)
        entrante = rede.nos[3]
        cliente = Cliente(entrante)
        await ate(rede, lambda: rede.nos[2].fase == INSTALADA)
        for _ in range(4):
            rede.tempo += 0.05
            for no in rede.nos.values():
                no.ciclo()
            rede.drenar()
        assert entrante.estado == SINCRONIZANDO
        assert not entrante._participa_da_eleicao()
        assert entrante.historico == [] and cliente.recebido == []
        assert entrante.aceitar_do_cliente(nova("não admitir cedo"))["tipo"] == "recusada"
        if bloquear != "VIEW_RESUME":
            assert rede.nos[2].estado != ATIVO
        else:
            # O coordenador pode retomar antes do RESUME chegar ao entrante.
            # MSG da nova época aguarda com 503 no canal FIFO.
            await ate(rede, lambda: rede.nos[2].estado == ATIVO)
            rede.nos[2].aceitar_do_cliente(nova("em trânsito até RESUME"))
            rede.drenar(limite=30)
            assert rede.recusas_503 > 0
        rede.perder = None
        await ate(rede, retomados(rede, (3, 2), (1, 2, 3)))
        rede.drenar()
        cliente.conferir(entrante)
        assert ids_entregues(entrante) == ids_entregues(rede.nos[1])

    asyncio.run(cenario())


def test_snapshot_de_outra_encarnacao_e_controles_antigos_nao_readmitem():
    async def cenario():
        rede, vivos, m = await preparar()
        antigos = copy.deepcopy(rede.controles)
        rede.perder = lambda o, d, p: p["tipo"] == "SNAPSHOT"
        reiniciar(rede, 3)
        await ate(rede, lambda: rede.nos[2].fase == INSTALADA)
        snapshot = rede.nos[2]._pacote_install(rede.nos[2]._transicao)
        snapshot.update(tipo="SNAPSHOT", encarnacao="encarnação-anterior")
        assert "ignorado" in rede.nos[3].receber_pacote(snapshot)[1]
        assert rede.nos[3].visao is None
        rede.perder = None
        await ate(rede, retomados(rede, (3, 2), (1, 2, 3)))
        for origem, destino, pacote in antigos:
            if origem == 3:
                assert rede.nos[destino].receber_pacote(pacote)[0] == 200
        assert all(n.estado == ATIVO and n.suspeitos == set() for n in rede.nos.values())

    asyncio.run(cenario())


def test_reinicio_rapido_remove_depois_readmite_e_permite_nova_queda():
    async def cenario():
        rede = grupo()
        for no in rede.nos.values():
            no.ciclo()
        rede.drenar()
        m = nova("confirmada antes de reiniciar")
        rede.nos[1].aceitar_do_cliente(m)
        rede.drenar()
        reiniciar(rede, 3)  # sem esperar timeout; mudança de encarnação exige primeiro a T05
        await ate(rede, retomados(rede, (3, 2), (1, 2, 3)))
        await ate(rede, lambda: all(n._transicao is None for n in rede.nos.values()))
        rede.mortos.add(1)
        await ate(rede, retomados(rede, (4, 3), (2, 3)))
        reiniciar(rede, 1)
        await ate(rede, retomados(rede, (5, 3), (1, 2, 3)))
        assert all(ids_entregues(n) == [m["id"]] for n in rede.nos.values())

    asyncio.run(cenario())


@pytest.mark.parametrize("defeito", ["sem pedido", "origem", "lider", "membros", "epoca", "uuid", "relogio"])
def test_snapshot_incompativel_nao_instala_nem_publica(defeito):
    async def cenario():
        rede, vivos, m = await preparar()
        rede.perder = lambda o, d, p: p["tipo"] == "SNAPSHOT"
        reiniciar(rede, 3)
        await ate(rede, lambda: rede.nos[2].fase == INSTALADA)
        no = rede.nos[3]
        pacote = rede.nos[2]._pacote_install(rede.nos[2]._transicao)
        pacote.update(tipo="SNAPSHOT", encarnacao=no.encarnacao)
        if defeito == "sem pedido":
            no._pedido_join = None
        elif defeito == "origem":
            pacote["origem"] = 1
        elif defeito == "lider":
            pacote["lider"] = 2
        elif defeito == "membros":
            pacote["membros"] = [2, 3]
        elif defeito == "epoca":
            pacote["transicao"] = pacote["anterior"]
        elif defeito == "uuid":
            pacote["historico"] = pacote["historico"] * 2
        elif defeito == "relogio":
            pacote["relogio"] = 0
        assert "ignorado" in no.receber_pacote(pacote)[1]
        assert no.visao is None and no.estado == SINCRONIZANDO
        assert no.historico == [] and no._a_publicar == [] and no.relogio.valor == 0

    asyncio.run(cenario())
