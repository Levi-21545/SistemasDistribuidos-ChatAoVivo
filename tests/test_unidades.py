from app.config import ler_nos
from app.modelos import Evento, validar_mensagem
from app.ordenacao import FilaOrdenada
from app.relogio import RelogioLamport

UUID_A = "00000000-0000-4000-8000-00000000000a"
UUID_B = "00000000-0000-4000-8000-00000000000b"


def msg(uuid):
    return {"id": uuid, "sala": "geral", "autor": "ana", "texto": "oi"}


def test_relogio_lamport():
    r = RelogioLamport()
    assert r.tick() == 1
    assert r.receber(10) == 11
    assert r.receber(3) == 12
    r.avancar_para(5)
    assert r.valor == 12  # nunca reduz
    r.avancar_para(20)
    assert r.valor == 20


def test_ler_nos():
    assert ler_nos("1=http://no1:8000, 2=http://no2:8000/") == {1: "http://no1:8000", 2: "http://no2:8000"}


def test_validacao_da_mensagem():
    ok, erro = validar_mensagem(msg(UUID_A.upper()))
    assert erro is None and ok["id"] == UUID_A  # UUID normalizado
    assert validar_mensagem({**msg(UUID_A), "texto": "  "})[1]
    assert validar_mensagem({**msg("nao-e-uuid")})[1]
    assert validar_mensagem("texto")[1]


def test_fila_so_entrega_com_ack_de_todos_e_na_ordem_do_carimbo():
    fila = FilaOrdenada({1, 2, 3})
    b = Evento((1, 3), 5, 2, msg(UUID_B))
    a = Evento((1, 3), 5, 1, msg(UUID_A))  # mesmo t: desempate pela origem
    fila.adicionar(b)
    fila.adicionar(a)
    for membro in (1, 2, 3):
        fila.registrar_ack(b.chave, membro)
    assert fila.retirar_prontos() == []  # b está completo, mas a (5,1) vem antes
    fila.registrar_ack(a.chave, 2)
    assert fila.retirar_prontos() == []
    fila.registrar_ack(a.chave, 3)
    assert [e.uuid for e in fila.retirar_prontos()] == [UUID_A, UUID_B]


def test_ack_antes_do_msg_fica_guardado():
    fila = FilaOrdenada({1, 2})
    a = Evento((1, 2), 4, 1, msg(UUID_A))
    fila.registrar_ack(a.chave, 2)
    fila.adicionar(a)
    assert [e.uuid for e in fila.retirar_prontos()] == [UUID_A]
