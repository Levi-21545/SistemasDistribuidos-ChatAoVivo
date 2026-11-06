"""Reconciliação pura dos estados capturados no corte (T05)."""

import pytest

from app.modelos import Evento
from app.reconciliacao import ErroReconciliacao, reconciliar
from tests.test_unidades import UUID_A, UUID_B, msg

UUID_C = "00000000-0000-4000-8000-00000000000c"
ANTIGA, ENCERRADA = (1, 3), (2, 3)


def ev(uuid, relogio, origem, epoca=ENCERRADA, texto="oi"):
    return Evento(epoca, relogio, origem, {**msg(uuid), "texto": texto}).json()


def estado(historico=(), pendentes=(), relogio=0):
    return {"historico": list(historico), "pendentes": list(pendentes), "relogio": relogio}


def uuids(historico):
    return [e.uuid for e in historico]


def test_base_mais_longa_e_pendencias_pela_chave_original():
    velho = ev(UUID_A, 1, 3, epoca=ANTIGA)  # visão anterior: preservado como está
    x = ev(UUID_B, 10, 1)
    y = ev(UUID_C, 20, 3)
    historico, relogio = reconciliar({1: estado([velho, x], [y], 21), 2: estado([velho], [x], 15)}, ENCERRADA)
    assert [e.json() for e in historico] == [velho, x, y]
    assert relogio == 21


def test_pendencias_de_varios_nos_entram_ordenadas_sem_repetir():
    a, b = ev(UUID_A, 7, 2), ev(UUID_B, 7, 1)  # mesmo relógio: desempate pela origem
    historico, _ = reconciliar({1: estado([], [a]), 2: estado([], [b, a])}, ENCERRADA)
    assert uuids(historico) == [UUID_B, UUID_A]


def test_uuid_ja_ocorrido_fica_fora():
    primeira = ev(UUID_A, 3, 1)
    segunda = ev(UUID_A, 9, 2)  # mesmo UUID aceito por outro nó
    historico, _ = reconciliar({1: estado([primeira], [segunda]), 2: estado([], [primeira, segunda])},
                               ENCERRADA)
    assert [e.json() for e in historico] == [primeira]


def test_duplicata_ja_resolvida_antes_de_uma_entrega_fica_fora():
    a1, a6, b = ev(UUID_A, 1, 1), ev(UUID_A, 6, 2), ev(UUID_B, 11, 1)
    historico, _ = reconciliar({1: estado([a1, b]), 2: estado([a1], [a6, b])}, ENCERRADA)
    assert [e.json() for e in historico] == [a1, b]


def test_duplicata_resolvida_com_outro_conteudo_mantem_a_primeira():
    a1, b = ev(UUID_A, 1, 1), ev(UUID_B, 11, 1)
    a6 = ev(UUID_A, 6, 2, texto="outro")
    historico, _ = reconciliar({1: estado([a1, b]), 2: estado([a1], [a6, b])}, ENCERRADA)
    assert [e.json() for e in historico] == [a1, b]


def test_ocorrencia_anterior_a_primeira_confirmada_e_recusada():
    """Uma pendência com o mesmo UUID e chave menor contradiria qual ocorrência valeu."""
    a5 = ev(UUID_A, 5, 1)
    with pytest.raises(ErroReconciliacao, match="anterior à última mensagem já entregue"):
        reconciliar({1: estado([a5]), 2: estado([], [ev(UUID_A, 3, 2)])}, ENCERRADA)


def test_historicos_divergentes_sao_recusados():
    with pytest.raises(ErroReconciliacao, match="divergem na posição 1"):
        reconciliar({1: estado([ev(UUID_A, 1, 1)]), 2: estado([ev(UUID_B, 1, 1)])}, ENCERRADA)


def test_mesmo_evento_com_conteudos_diferentes_e_recusado():
    with pytest.raises(ErroReconciliacao, match="conteúdos diferentes"):
        reconciliar({1: estado([], [ev(UUID_A, 4, 1)]), 2: estado([], [ev(UUID_A, 4, 1, texto="outro")])},
                    ENCERRADA)


def test_pendencia_anterior_a_uma_entrega_nao_reescreve_a_ordem():
    entregue = ev(UUID_A, 10, 1)
    with pytest.raises(ErroReconciliacao, match="anterior à última mensagem já entregue"):
        reconciliar({1: estado([entregue]), 2: estado([], [ev(UUID_B, 5, 2)])}, ENCERRADA)


def test_pendencia_de_outra_epoca_e_recusada():
    with pytest.raises(ErroReconciliacao, match="fora da visão encerrada"):
        reconciliar({1: estado([], [ev(UUID_A, 1, 1, epoca=ANTIGA)])}, ENCERRADA)
