"""T02/T03: MSG/ACK com Lamport em visão fixa, sob intercalações e repetições."""

import uuid

import pytest

from app.no import ATIVO
from tests.simulacao import criar_grupo, ids_entregues, mensagem


def rodar_cenario(seed: int, total: int = 30, prob_duplicar: float = 0.2, retomada_tardia=None):
    """Clientes enviam mensagens a nós aleatórios enquanto a rede entrega pacotes."""
    ativos = None if retomada_tardia is None else {1, 2, 3} - {retomada_tardia}
    rede = criar_grupo(seed=seed, prob_duplicar=prob_duplicar, ativos=ativos)
    rng = rede.rng
    enviados = []
    passos = 0
    while len(enviados) < total or rede.ha_pacotes():
        passos += 1
        if retomada_tardia is not None and passos == 40:
            rede.nos[retomada_tardia].retomar()
        if len(enviados) < total and rng.random() < 0.3:
            no = rede.nos[rng.choice([n for n, x in rede.nos.items() if x.estado == ATIVO])]
            m = mensagem(str(uuid.UUID(int=rng.getrandbits(128), version=4)), f"m{len(enviados)}")
            assert no.aceitar_do_cliente(m)["tipo"] == "aceita"
            enviados.append(m["id"])
        else:
            rede.passo()
        assert passos < 100_000
    return rede, enviados


@pytest.mark.parametrize("seed", range(200))
def test_todos_entregam_a_mesma_sequencia(seed):
    rede, enviados = rodar_cenario(seed)
    sequencias = {n: ids_entregues(no) for n, no in rede.nos.items()}
    assert sequencias[1] == sequencias[2] == sequencias[3]
    assert sorted(sequencias[1]) == sorted(enviados)  # tudo entregue, sem duplicatas
    for no in rede.nos.values():
        assert no.fila.pendentes() == []


def test_ordem_respeita_os_carimbos_originais():
    rede, _ = rodar_cenario(seed=7)
    ordens = [e.ordem for e in rede.nos[1].historico]
    assert ordens == sorted(ordens)


def test_repeticoes_nao_geram_efeitos():
    rede, enviados = rodar_cenario(seed=3, prob_duplicar=1.0)
    assert rede.duplicatas > 0
    assert len(ids_entregues(rede.nos[2])) == len(enviados)


@pytest.mark.parametrize("seed", range(20))
def test_no_que_retoma_depois_recebe_tudo_via_retry(seed):
    # O nó 3 responde 503 até retomar; os canais repetem o pacote sem perder a ordem FIFO.
    rede, enviados = rodar_cenario(seed, retomada_tardia=3)
    assert ids_entregues(rede.nos[1]) == ids_entregues(rede.nos[2]) == ids_entregues(rede.nos[3])
    assert sorted(ids_entregues(rede.nos[3])) == sorted(enviados)


def test_mesmo_uuid_em_nos_distintos_entra_uma_vez():
    rede = criar_grupo(seed=1)
    m = mensagem(str(uuid.uuid4()), "repetida")
    assert rede.nos[1].aceitar_do_cliente(m)["tipo"] == "aceita"
    assert rede.nos[2].aceitar_do_cliente(m)["tipo"] == "aceita"
    while rede.passo():
        pass
    for no in rede.nos.values():
        assert ids_entregues(no) == [m["id"]]
    # Depois da entrega, reenviar o mesmo UUID devolve a confirmação.
    assert rede.nos[3].aceitar_do_cliente(m) == {"tipo": "confirmada", "id": m["id"]}


def test_uuid_reutilizado_com_outro_conteudo_e_recusado():
    rede = criar_grupo()
    m = mensagem(str(uuid.uuid4()), "original")
    rede.nos[1].aceitar_do_cliente(m)
    resposta = rede.nos[1].aceitar_do_cliente({**m, "texto": "alterado"})
    assert resposta["tipo"] == "recusada"


def test_pacote_de_epoca_antiga_e_descartado_e_futura_aguarda():
    rede = criar_grupo()
    no = rede.nos[1]
    base = {"tipo": "ACK", "origem": 2, "relogio": 1, "evento": {"origem": 3, "relogio": 1}}
    assert no.receber_pacote({**base, "pacote_id": "a", "epoca": [0, 3]}) == (200, {"descartado": "época antiga"})
    assert no.receber_pacote({**base, "pacote_id": "b", "epoca": [2, 3]})[0] == 503


def test_no_pausado_recusa_cliente():
    rede = criar_grupo(ativos={1, 2})
    resposta = rede.nos[3].aceitar_do_cliente(mensagem(str(uuid.uuid4()), "oi"))
    assert resposta["tipo"] == "recusada"
