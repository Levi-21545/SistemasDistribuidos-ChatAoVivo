"""Pacotes malformados são recusados com 400 antes de qualquer efeito."""

import copy
import uuid

import pytest

from app.config import Config
from app.no import No
from tests.simulacao import criar_grupo, mensagem

EPOCA = [1, 3]


def msg(**campos):
    base = {"tipo": "MSG", "pacote_id": "2-teste-1", "epoca": EPOCA, "origem": 2, "relogio": 5,
            "mensagem": mensagem(str(uuid.uuid4()), "oi")}
    return base | campos


def ack(**campos):
    base = {"tipo": "ACK", "pacote_id": "2-teste-2", "epoca": EPOCA, "origem": 2, "relogio": 5,
            "evento": {"origem": 3, "relogio": 1}}
    return base | campos


def controle(tipo, **campos):
    return {"tipo": tipo, "pacote_id": "2-teste-3", "epoca": EPOCA, "origem": 2} | campos


def sem(pacote, campo):
    return {k: v for k, v in pacote.items() if k != campo}


def foto(rede, no):
    """Tudo o que um pacote poderia alterar no nó ou emitir para a rede."""
    return {
        "relogio": no.relogio.valor,
        "pendentes": [e.json() for e in no.fila.pendentes()],
        "acks": copy.deepcopy(no.fila._acks),
        "historico": [e.json() for e in no.historico],
        "entregues": set(no.entregues),
        "processados": set(no._processados),
        "estado": no.estado,
        "coordenador": no.coordenador,
        "fase": no.fase,
        "transicao": no.estado_publico()["transicao"],
        "canais": {c: list(f) for c, f in rede.canais.items()},
        "diretos": list(rede.diretos),
    }


INSTALL = controle("VIEW_INSTALL", transicao=EPOCA, anterior=None, lider=3, membros=[1, 2, 3], historico=[],
                   relogio=0)

PREPARE = controle("VIEW_PREPARE", origem=3, transicao=[2, 3], anterior=EPOCA, membros=[1, 3])
STATE = controle("VIEW_STATE", transicao=[2, 3], anterior=EPOCA, historico=[], pendentes=[], relogio=0)
JOIN = controle("JOIN", epoca=None, anterior=EPOCA, encarnacao="teste")
SNAPSHOT = INSTALL | {"tipo": "SNAPSHOT", "anterior": EPOCA, "encarnacao": "teste"}

MALFORMADOS = {
    "JOIN sem anterior": sem(JOIN, "anterior"),
    "JOIN anterior null": JOIN | {"anterior": None},
    "JOIN sem encarnacao": sem(JOIN, "encarnacao"),
    "JOIN encarnacao vazia": JOIN | {"encarnacao": ""},
    "SNAPSHOT sem encarnacao": sem(SNAPSHOT, "encarnacao"),
    "SNAPSHOT encarnacao numero": SNAPSHOT | {"encarnacao": 3},
    "SNAPSHOT sem anterior": sem(SNAPSHOT, "anterior"),
    "SNAPSHOT anterior null": SNAPSHOT | {"anterior": None},
    "SNAPSHOT membros booleanos": SNAPSHOT | {"membros": [True, 2, 3]},
    "SNAPSHOT historico malformado": SNAPSHOT | {"historico": [{}]},
    "SNAPSHOT relogio booleano": SNAPSHOT | {"relogio": True},
    "SNAPSHOT sem transicao": sem(SNAPSHOT, "transicao"),
    "MSG sem pacote_id": sem(msg(), "pacote_id"),
    "ACK sem pacote_id": sem(ack(), "pacote_id"),
    "pacote_id número": msg(pacote_id=7),
    "pacote_id lista": ack(pacote_id=["2-teste-2"]),
    "pacote_id vazio": msg(pacote_id=""),
    "pacote_id null": ack(pacote_id=None),
    "tipo lista": msg(tipo=["MSG"]),
    "origem booleana": msg(origem=True),
    "origem texto": ack(origem="2"),
    "MSG sem epoca": sem(msg(), "epoca"),
    "MSG epoca null": msg(epoca=None),
    "ACK epoca curta": ack(epoca=[1]),
    "MSG epoca com texto": msg(epoca=["1", 3]),
    "ACK epoca booleana": ack(epoca=[True, 3]),
    "MSG sem relogio": sem(msg(), "relogio"),
    "MSG relogio texto": msg(relogio="5"),
    "MSG relogio booleano": msg(relogio=True),
    "MSG relogio float": msg(relogio=5.0),
    "MSG sem mensagem": sem(msg(), "mensagem"),
    "MSG mensagem sem UUID": msg(mensagem=mensagem("nao-e-uuid", "oi")),
    "MSG mensagem texto": msg(mensagem="oi"),
    "ACK sem relogio": sem(ack(), "relogio"),
    "ACK sem evento": sem(ack(), "evento"),
    "ACK evento incompleto": ack(evento={"origem": 3}),
    "ACK evento com texto": ack(evento={"origem": "3", "relogio": 1}),
    "ACK evento lista": ack(evento=[3, 1]),
    "HEARTBEAT sem estado": controle("HEARTBEAT", coordenador=3),
    "HEARTBEAT estado desconhecido": controle("HEARTBEAT", estado="morto", coordenador=3),
    "HEARTBEAT sem coordenador": controle("HEARTBEAT", estado="ativo"),
    "HEARTBEAT coordenador texto": controle("HEARTBEAT", estado="ativo", coordenador="3"),
    "VIEW_INSTALL sem transicao": sem(INSTALL, "transicao"),
    "VIEW_INSTALL lider booleano": INSTALL | {"lider": True},
    "VIEW_INSTALL membros texto": INSTALL | {"membros": ["1", "2", "3"]},
    "VIEW_INSTALL historico malformado": INSTALL | {"historico": [{"epoca": EPOCA, "relogio": 1}]},
    "VIEW_INSTALL sem relogio": sem(INSTALL, "relogio"),
    "VIEW_READY transicao null": controle("VIEW_READY", transicao=None),
    "VIEW_RESUME transicao curta": controle("VIEW_RESUME", transicao=[1]),
    "VIEW_INSTALL sem anterior": sem(INSTALL, "anterior"),
    "VIEW_INSTALL anterior curta": INSTALL | {"anterior": [1]},
    "VIEW_PREPARE sem anterior": sem(PREPARE, "anterior"),
    "VIEW_PREPARE anterior null": PREPARE | {"anterior": None},
    "VIEW_PREPARE transicao texto": PREPARE | {"transicao": ["2", 3]},
    "VIEW_PREPARE sem membros": sem(PREPARE, "membros"),
    "VIEW_PREPARE membros booleanos": PREPARE | {"membros": [True, 3]},
    "VIEW_STATE sem pendentes": sem(STATE, "pendentes"),
    "VIEW_STATE pendente malformada": STATE | {"pendentes": [{"epoca": EPOCA, "relogio": 1, "origem": 2}]},
    "VIEW_STATE historico texto": STATE | {"historico": "[]"},
    "VIEW_STATE relogio float": STATE | {"relogio": 1.0},
    "VIEW_STATE sem transicao": sem(STATE, "transicao"),
}


@pytest.mark.parametrize("pacote", MALFORMADOS.values(), ids=MALFORMADOS.keys())
def test_pacote_malformado_e_recusado_sem_efeitos(pacote):
    rede = criar_grupo()
    no = rede.nos[1]
    rede.nos[2].aceitar_do_cliente(mensagem(str(uuid.uuid4()), "pendente"))  # estado não trivial
    while rede.passo():
        pass
    antes = foto(rede, no)

    status, corpo = no.receber_pacote(copy.deepcopy(pacote))

    assert status == 400, corpo
    assert foto(rede, no) == antes


def test_formatos_validos_continuam_aceitos():
    rede = criar_grupo()
    no = rede.nos[1]
    assert no.receber_pacote(msg()) == (200, {"ok": True})
    assert no.receber_pacote(ack()) == (200, {"ok": True})
    assert no.receber_pacote(msg()) == (200, {"duplicado": True})
    assert no.receber_pacote(controle("HEARTBEAT", estado="ativo", coordenador=3))[0] == 200
    for tipo in ("ELECTION", "OK", "COORDINATOR"):
        assert no.receber_pacote(controle(tipo))[0] == 200
    for tipo in ("VIEW_READY", "VIEW_RESUME"):
        assert no.receber_pacote(controle(tipo, transicao=EPOCA))[0] == 200
    assert no.receber_pacote(STATE) == (200, {"ignorado": "estado de transição desconhecida ou já reconciliada"})
    assert no.receber_pacote(PREPARE)[0] == 200 and no.fase == "corte"  # proposta válida do líder 3


def test_controles_com_epoca_null_continuam_aceitos():
    """Um nó sem visão envia heartbeat e controles com epoca null (contrato v1)."""
    rede = criar_grupo()
    no = rede.nos[1]
    heartbeat = controle("HEARTBEAT", epoca=None, estado="sincronizando", coordenador=None)
    assert no.receber_pacote(heartbeat) == (200, {"ok": True})
    assert no.estado_pares[2] == {"estado": "sincronizando", "epoca": None, "coordenador": None}
    for tipo in ("ELECTION", "OK", "COORDINATOR"):
        assert no.receber_pacote(controle(tipo, epoca=None))[0] == 200
    for tipo in ("VIEW_READY", "VIEW_RESUME"):
        assert no.receber_pacote(controle(tipo, epoca=None, transicao=EPOCA))[0] == 200


def test_view_install_do_bootstrap_continua_aceito():
    rede = criar_grupo()
    no = No(Config(no_id=1, nos={i: f"http://no{i}" for i in (1, 2, 3)}), rede.para(1), agora=lambda: 0.0)
    historico = [{"epoca": EPOCA, "relogio": 1, "origem": 3, "mensagem": mensagem(str(uuid.uuid4()), "oi")}]
    install = INSTALL | {"origem": 3, "historico": historico, "relogio": 4}
    assert no.receber_pacote(install) == (200, {"ok": True})
    assert no.visao.epoca == (1, 3) and no.relogio.valor == 4
    assert no.historico == []  # o histórico instalado só é publicado no VIEW_RESUME
    assert no.receber_pacote(controle("VIEW_RESUME", origem=3, transicao=EPOCA)) == (200, {"ok": True})
    assert len(no.historico) == 1
