"""Estado e regras de um nó.

As funções que alteram estado são síncronas: o asyncio executa uma de cada vez,
então a atualização do estado e o enfileiramento de cada multicast ficam
serializados sem travas. Só a eleição (que espera timeouts) é assíncrona.

Estados:
  sincronizando  sem visão instalada ou aguardando VIEW_RESUME; fora da eleição.
  ativo          aceita mensagens de clientes.
  reorganizando  suspeita de falha ou eleição em curso; novos envios pausados.
"""

import asyncio
import logging
import time
import uuid as uuidlib
from collections.abc import Callable

from .config import Config
from .modelos import Evento, Visao, validar_mensagem
from .ordenacao import FilaOrdenada
from .relogio import RelogioLamport

log = logging.getLogger(__name__)

SINCRONIZANDO = "sincronizando"
ATIVO = "ativo"
REORGANIZANDO = "reorganizando"

Resposta = tuple[int, dict]  # (status HTTP, corpo JSON)


class No:
    def __init__(self, cfg: Config, rede, agora: Callable[[], float] = time.monotonic):
        self.cfg = cfg
        self.id = cfg.no_id
        self.rede = rede
        self.agora = agora

        self.estado = SINCRONIZANDO
        self.visao: Visao | None = None
        self.coordenador: int | None = None
        self.relogio = RelogioLamport()
        self.fila: FilaOrdenada | None = None
        self.historico: list[Evento] = []
        self.entregues: dict[str, Evento] = {}
        self.ouvintes: list[Callable[[Evento, int], None]] = []

        # Identidade de pacotes: a encarnação muda a cada reinício do processo,
        # então um nó reiniciado nunca repete um pacote_id antigo.
        self.encarnacao = uuidlib.uuid4().hex[:8]
        self._seq = 0
        self._processados: set[str] = set()

        self.ultimo_contato: dict[int, float] = {}
        self.estado_pares: dict[int, dict] = {}
        self.suspeitos: set[int] = set()
        self._eleicao_ativa = False
        self._recebeu_ok = False
        self._anuncio = asyncio.Event()

        self._transicao: Visao | None = None  # visão proposta por este nó (bootstrap)
        self._prontos: set[int] = set()
        self._tarefas: set[asyncio.Task] = set()

        self._tratadores = {
            "MSG": self._receber_msg,
            "ACK": self._receber_ack,
            "HEARTBEAT": self._receber_heartbeat,
            "ELECTION": self._receber_election,
            "OK": self._receber_ok,
            "COORDINATOR": self._receber_coordinator,
            "VIEW_INSTALL": self._receber_view_install,
            "VIEW_READY": self._receber_view_ready,
            "VIEW_RESUME": self._receber_view_resume,
        }

    # ------------------------------------------------------------------ envelope

    def _pacote(self, tipo: str, **campos) -> dict:
        self._seq += 1
        return {
            "tipo": tipo,
            "pacote_id": f"{self.id}-{self.encarnacao}-{self._seq}",
            "epoca": list(self.visao.epoca) if self.visao else None,
            "origem": self.id,
            **campos,
        }

    def _outros_membros(self) -> list[int]:
        return sorted(self.visao.membros - {self.id}) if self.visao else []

    def _mesma_epoca(self, pacote: dict) -> bool:
        return self.visao is not None and pacote.get("epoca") == list(self.visao.epoca)

    def _agendar(self, corotina) -> None:
        tarefa = asyncio.get_running_loop().create_task(corotina)
        self._tarefas.add(tarefa)
        tarefa.add_done_callback(self._tarefas.discard)

    # ------------------------------------------------------------------ cliente

    def aceitar_do_cliente(self, dados) -> dict:
        """Recebe uma tentativa do cliente e devolve a resposta imediata.

        "aceita" não é confirmação: a confirmação é a entrega da mensagem.
        Reenviar o mesmo UUID é seguro e serve também como consulta.
        """
        mensagem, erro = validar_mensagem(dados)
        if erro:
            ident = dados.get("id") if isinstance(dados, dict) else None
            return {"tipo": "recusada", "id": ident, "motivo": erro}
        uuid = mensagem["id"]

        conhecido = self.entregues.get(uuid) or (self.fila.por_uuid(uuid) if self.fila else None)
        if conhecido is not None:
            if conhecido.mensagem != mensagem:
                return {"tipo": "recusada", "id": uuid, "motivo": "UUID reutilizado com conteúdo diferente"}
            if uuid in self.entregues:
                return {"tipo": "confirmada", "id": uuid}
            return {"tipo": "aceita", "id": uuid}

        if self.estado != ATIVO:
            return {"tipo": "recusada", "id": uuid, "motivo": f"nó {self.id} está {self.estado}; tente novamente"}

        t = self.relogio.tick()
        evento = Evento(self.visao.epoca, t, self.id, mensagem)
        self.fila.adicionar(evento)
        pacote = self._pacote("MSG", relogio=t, mensagem=mensagem)
        for destino in self._outros_membros():
            self.rede.enfileirar(destino, pacote)
        self._tentar_entregar()
        return {"tipo": "aceita", "id": uuid}

    # ------------------------------------------------------------------ pacotes

    def receber_pacote(self, pacote: dict) -> Resposta:
        tipo = pacote.get("tipo")
        tratador = self._tratadores.get(tipo) if isinstance(tipo, str) else None
        if tratador is None:
            return 400, {"erro": f"tipo desconhecido: {tipo!r}"}
        if not _inteiro(pacote.get("origem")) or pacote["origem"] not in self.cfg.nos:
            return 400, {"erro": "origem desconhecida"}
        # Valida tudo antes de qualquer efeito: o tratador não pode falhar no meio.
        erro = erro_de_formato(pacote)
        if erro:
            return 400, {"erro": f"pacote malformado: {erro}"}
        dados = pacote["tipo"] in ("MSG", "ACK")
        if dados and pacote.get("pacote_id") in self._processados:
            return 200, {"duplicado": True}
        try:
            status, corpo = tratador(pacote)
        except (KeyError, TypeError, ValueError) as erro:
            return 400, {"erro": f"pacote malformado: {erro!r}"}
        if dados and status == 200:
            self._processados.add(pacote["pacote_id"])
        return status, corpo

    def _bloqueio_de_dados(self, pacote: dict) -> Resposta | None:
        """Decide se um MSG/ACK pode ser processado agora (None = pode)."""
        if self.visao is None or self.estado == SINCRONIZANDO:
            return 503, {"erro": "nó ainda não retomou a visão"}
        epoca = tuple(pacote["epoca"])
        if epoca < self.visao.epoca:
            return 200, {"descartado": "época antiga"}
        if epoca > self.visao.epoca:
            return 503, {"erro": "época ainda não instalada"}
        if pacote["origem"] not in self.visao.membros:
            return 200, {"descartado": "origem fora da visão"}
        return None

    def _receber_msg(self, pacote: dict) -> Resposta:
        bloqueio = self._bloqueio_de_dados(pacote)
        if bloqueio:
            return bloqueio
        mensagem, erro = validar_mensagem(pacote["mensagem"])
        if erro:
            return 400, {"erro": erro}
        t, origem = int(pacote["relogio"]), pacote["origem"]

        self.relogio.receber(t)
        evento = Evento(self.visao.epoca, t, origem, mensagem)  # chave usa o carimbo ORIGINAL
        self.fila.adicionar(evento)
        self.fila.registrar_ack(evento.chave, self.id)

        ack = self._pacote("ACK", relogio=self.relogio.tick(), evento={"origem": origem, "relogio": t})
        for destino in self._outros_membros():
            self.rede.enfileirar(destino, ack)
        self._tentar_entregar()
        return 200, {"ok": True}

    def _receber_ack(self, pacote: dict) -> Resposta:
        bloqueio = self._bloqueio_de_dados(pacote)
        if bloqueio:
            return bloqueio
        self.relogio.receber(int(pacote["relogio"]))
        ref = pacote["evento"]
        chave = (self.visao.epoca, int(ref["origem"]), int(ref["relogio"]))
        self.fila.registrar_ack(chave, pacote["origem"])
        self._tentar_entregar()
        return 200, {"ok": True}

    def _tentar_entregar(self) -> None:
        for evento in self.fila.retirar_prontos():
            self._entregar(evento)

    def _entregar(self, evento: Evento) -> None:
        anterior = self.entregues.get(evento.uuid)
        if anterior is not None:
            # Mesmo UUID aceito por nós distintos: vale a primeira ocorrência na ordem total.
            if anterior.mensagem != evento.mensagem:
                log.warning("UUID %s reutilizado com conteúdo diferente; mantida a primeira", evento.uuid)
            return
        self.historico.append(evento)
        self.entregues[evento.uuid] = evento
        for ouvinte in self.ouvintes:
            ouvinte(evento, len(self.historico))

    # ------------------------------------------------------------------ visão

    def instalar(self, visao: Visao, historico: list[Evento], relogio: int) -> None:
        """Instala a visão e permanece pausado até retomar()."""
        self.visao = visao
        self.coordenador = visao.lider
        self.fila = FilaOrdenada(visao.membros)
        self.historico = list(historico)
        self.entregues = {e.uuid: e for e in historico}
        self.relogio.avancar_para(relogio)
        self.suspeitos.clear()
        log.info("visão instalada: época %s, líder %s, membros %s",
                 visao.epoca, visao.lider, sorted(visao.membros))

    def retomar(self) -> None:
        self.estado = ATIVO
        agora = self.agora()
        for membro in self._outros_membros():
            self.ultimo_contato[membro] = agora  # carência para o detector de falhas
        log.info("nó ativo na época %s", self.visao.epoca)

    def _pausar_aplicacao(self, motivo: str) -> None:
        if self.estado == ATIVO:
            self.estado = REORGANIZANDO
            log.warning("envios pausados: %s", motivo)

    # Bootstrap: o nó de maior ID configurado propõe a primeira visão quando
    # ouviu todos os nós configurados e nenhum deles tem visão. Um nó que
    # reinicia depois encontra pares com visão e aguarda JOIN (T06).

    def _conduzir_bootstrap(self) -> None:
        if self.id != max(self.cfg.nos):
            return
        if self.visao is None and self._grupo_sem_visao():
            self._propor_visao_inicial()
        elif self._transicao is not None:
            self._avancar_transicao(reenviar=True)

    def _grupo_sem_visao(self) -> bool:
        agora = self.agora()
        for par in self.cfg.pares:
            visto = self.ultimo_contato.get(par)
            if visto is None or agora - visto > self.cfg.suspeita_timeout:
                return False
            if self.estado_pares.get(par, {}).get("epoca", "?") is not None:
                return False
        return True

    def _propor_visao_inicial(self) -> None:
        proposta = Visao(epoca=(1, self.id), lider=self.id, membros=frozenset(self.cfg.nos))
        log.info("bootstrap: propondo a visão inicial %s", proposta.json())
        self._transicao = proposta
        self._prontos = {self.id}
        self.instalar(proposta, historico=[], relogio=0)
        self._avancar_transicao(reenviar=True)

    def _avancar_transicao(self, reenviar: bool) -> None:
        """INSTALL até todos confirmarem READY; depois RESUME até todos estarem ativos."""
        proposta = self._transicao
        outros = sorted(proposta.membros - {self.id})
        faltam_prontos = sorted(proposta.membros - self._prontos)
        if faltam_prontos:
            if reenviar:
                for membro in faltam_prontos:
                    self.rede.enviar_direto(membro, self._pacote_install(proposta))
            return
        if self.estado == SINCRONIZANDO:
            self.retomar()
            for membro in outros:
                self.rede.enviar_direto(membro, self._pacote_resume(proposta))
            return
        faltam_ativos = [m for m in outros if not self._par_ativo_em(m, proposta)]
        if not faltam_ativos:
            log.info("transição %s concluída", proposta.epoca)
            self._transicao = None
        elif reenviar:
            for membro in faltam_ativos:
                self.rede.enviar_direto(membro, self._pacote_resume(proposta))

    def _par_ativo_em(self, par: int, visao: Visao) -> bool:
        estado = self.estado_pares.get(par, {})
        return estado.get("estado") != SINCRONIZANDO and estado.get("epoca") == list(visao.epoca)

    def _pacote_install(self, proposta: Visao) -> dict:
        return self._pacote(
            "VIEW_INSTALL",
            transicao=list(proposta.epoca),
            lider=proposta.lider,
            membros=sorted(proposta.membros),
            historico=[e.json() for e in self.historico],
            relogio=self.relogio.valor,
        )

    def _pacote_resume(self, proposta: Visao) -> dict:
        return self._pacote("VIEW_RESUME", transicao=list(proposta.epoca))

    def _receber_view_install(self, pacote: dict) -> Resposta:
        transicao = tuple(pacote["transicao"])
        proposta = Visao(transicao, int(pacote["lider"]), frozenset(int(m) for m in pacote["membros"]))
        if self.id not in proposta.membros:
            return 200, {"ignorado": "nó fora da visão proposta"}
        if self.visao is None:
            historico = [Evento.de_json(e) for e in pacote["historico"]]
            self.instalar(proposta, historico, int(pacote["relogio"]))
        elif self.visao.epoca != transicao:
            # Nesta versão só há instalação no bootstrap; a reorganização é T05.
            return 200, {"ignorado": "nó já possui outra visão"}
        self.rede.enviar_direto(pacote["origem"], self._pacote("VIEW_READY", transicao=list(transicao)))
        return 200, {"ok": True}

    def _receber_view_ready(self, pacote: dict) -> Resposta:
        if self._transicao is None or tuple(pacote["transicao"]) != self._transicao.epoca:
            return 200, {"ignorado": "transição desconhecida"}
        self._prontos.add(pacote["origem"])
        self._avancar_transicao(reenviar=False)
        return 200, {"ok": True}

    def _receber_view_resume(self, pacote: dict) -> Resposta:
        if self.visao is None or tuple(pacote["transicao"]) != self.visao.epoca:
            return 200, {"ignorado": "transição desconhecida"}
        if self.estado == SINCRONIZANDO:
            self.retomar()
        return 200, {"ok": True}

    # ------------------------------------------------------------------ heartbeat

    async def executar_ciclos(self) -> None:
        while True:
            self.ciclo()
            await asyncio.sleep(self.cfg.heartbeat_intervalo)

    def ciclo(self) -> None:
        for destino in self.cfg.pares:
            self.rede.enviar_direto(destino, self._pacote_heartbeat())
        self._detectar_falhas()
        self._conduzir_bootstrap()

    def _pacote_heartbeat(self) -> dict:
        return self._pacote("HEARTBEAT", estado=self.estado, coordenador=self.coordenador)

    def _receber_heartbeat(self, pacote: dict) -> Resposta:
        origem = pacote["origem"]
        self.ultimo_contato[origem] = self.agora()
        self.estado_pares[origem] = {"estado": pacote["estado"], "epoca": pacote["epoca"],
                                     "coordenador": pacote.get("coordenador")}
        return 200, {"ok": True}

    def _detectar_falhas(self) -> None:
        if self.visao is None or self.estado == SINCRONIZANDO:
            return
        agora = self.agora()
        for membro in self._outros_membros():
            silencio = agora - self.ultimo_contato.get(membro, agora)
            if membro not in self.suspeitos and silencio > self.cfg.suspeita_timeout:
                self._suspeitar(membro, silencio)

    def _suspeitar(self, membro: int, silencio: float = 0.0) -> None:
        self.suspeitos.add(membro)
        log.warning("nó %s suspeito: sem heartbeat há %.1fs", membro, silencio)
        self._pausar_aplicacao(f"suspeita do nó {membro}")
        if membro == self.coordenador:
            self._agendar(self.iniciar_eleicao())

    # ------------------------------------------------------------------ Bully

    def _participa_da_eleicao(self) -> bool:
        return self.visao is not None and self.estado != SINCRONIZANDO

    async def iniciar_eleicao(self) -> None:
        if self._eleicao_ativa or not self._participa_da_eleicao():
            return
        self._eleicao_ativa = True
        try:
            while True:
                self._recebeu_ok = False
                self._anuncio.clear()
                maiores = [m for m in self._outros_membros() if m > self.id and m not in self.suspeitos]
                log.info("eleição iniciada; ELECTION para %s", maiores)
                for destino in maiores:
                    self.rede.enviar_direto(destino, self._pacote("ELECTION"))
                if maiores:
                    await asyncio.sleep(self.cfg.eleicao_timeout)
                if not self._recebeu_ok:
                    self._assumir_coordenacao()
                    return
                try:
                    await asyncio.wait_for(self._anuncio.wait(), self.cfg.coordenador_timeout)
                    return
                except asyncio.TimeoutError:
                    log.warning("houve OK, mas nenhum COORDINATOR; repetindo a eleição")
        finally:
            self._eleicao_ativa = False

    def _assumir_coordenacao(self) -> None:
        self.coordenador = self.id
        self._pausar_aplicacao("eleição concluída; aguardando reorganização")
        for destino in self._outros_membros():
            if destino not in self.suspeitos:
                self.rede.enviar_direto(destino, self._pacote("COORDINATOR"))
        log.warning("eleito coordenador; envios seguem pausados até a reorganização da visão")

    def _receber_election(self, pacote: dict) -> Resposta:
        # Nós sincronizando (ou de outra época) ficam fora: não respondem OK.
        if not self._participa_da_eleicao() or not self._mesma_epoca(pacote):
            return 200, {"ignorado": "fora da eleição"}
        if pacote["origem"] < self.id:
            self.rede.enviar_direto(pacote["origem"], self._pacote("OK"))
            self._pausar_aplicacao(f"eleição iniciada pelo nó {pacote['origem']}")
            self._agendar(self.iniciar_eleicao())
        return 200, {"ok": True}

    def _receber_ok(self, pacote: dict) -> Resposta:
        if self._participa_da_eleicao() and self._mesma_epoca(pacote):
            self._recebeu_ok = True
        return 200, {"ok": True}

    def _receber_coordinator(self, pacote: dict) -> Resposta:
        if not self._participa_da_eleicao() or not self._mesma_epoca(pacote):
            return 200, {"ignorado": "fora da eleição"}
        novo = pacote["origem"]
        if novo < self.id:
            # Um nó de ID maior e vivo contesta o anúncio, como no Bully.
            self._agendar(self.iniciar_eleicao())
            return 200, {"ok": True}
        self.coordenador = novo
        self._pausar_aplicacao(f"nó {novo} anunciou coordenação")
        self._anuncio.set()
        log.info("coordenador eleito: nó %s (envios seguem pausados)", novo)
        return 200, {"ok": True}

    # ------------------------------------------------------------------ observação

    def estado_publico(self) -> dict:
        agora = self.agora()
        return {
            "no": self.id,
            "encarnacao": self.encarnacao,
            "estado": self.estado,
            "visao": self.visao.json() if self.visao else None,
            "coordenador": self.coordenador,
            "relogio": self.relogio.valor,
            "suspeitos": sorted(self.suspeitos),
            "entregues": len(self.historico),
            "pendentes": [e.json() for e in self.fila.pendentes()] if self.fila else [],
            "ultimo_contato": {p: round(agora - t, 2) for p, t in sorted(self.ultimo_contato.items())},
            "filas_saida": self.rede.tamanhos(),
        }


# ---------------------------------------------------------------------- formato

def _inteiro(valor) -> bool:
    """int de fato: bool é subclasse de int, mas True não é um relógio nem um ID."""
    return type(valor) is int


def _epoca(valor) -> bool:
    return isinstance(valor, list) and len(valor) == 2 and all(_inteiro(v) for v in valor)


def _mensagem(valor) -> bool:
    return validar_mensagem(valor)[1] is None


def _ref_evento(valor) -> bool:
    return isinstance(valor, dict) and _inteiro(valor.get("origem")) and _inteiro(valor.get("relogio"))


def _evento_json(valor) -> bool:
    return (isinstance(valor, dict) and _epoca(valor.get("epoca")) and _ref_evento(valor)
            and _mensagem(valor.get("mensagem")))


def _lista(item_valido):
    return lambda valor: isinstance(valor, list) and all(item_valido(v) for v in valor)


# Campos de cada tipo além do envelope (contrato v1), com o teste de cada um.
_CAMPOS = {
    "MSG": {"relogio": _inteiro, "mensagem": _mensagem},
    "ACK": {"relogio": _inteiro, "evento": _ref_evento},
    "HEARTBEAT": {"estado": lambda v: v in (SINCRONIZANDO, ATIVO, REORGANIZANDO),
                  "coordenador": lambda v: v is None or _inteiro(v)},
    "ELECTION": {},
    "OK": {},
    "COORDINATOR": {},
    "VIEW_INSTALL": {"transicao": _epoca, "lider": _inteiro, "membros": _lista(_inteiro),
                     "historico": _lista(_evento_json), "relogio": _inteiro},
    "VIEW_READY": {"transicao": _epoca},
    "VIEW_RESUME": {"transicao": _epoca},
}


def erro_de_formato(pacote: dict) -> str | None:
    """Confere o envelope e os campos do tipo; devolve o problema ou None.

    `tipo` e `origem` já foram conferidos por receber_pacote.
    """
    if not isinstance(pacote.get("pacote_id"), str) or not pacote["pacote_id"]:
        return "pacote_id deve ser um texto não vazio"
    if "epoca" not in pacote:
        return "epoca ausente"
    # Época null só vale nos controles: um nó sem visão ainda envia heartbeat.
    nula_permitida = pacote["tipo"] not in ("MSG", "ACK")
    if not (_epoca(pacote["epoca"]) or (pacote["epoca"] is None and nula_permitida)):
        return "epoca deve ser [contador, coordenador]" + (" ou null" if nula_permitida else "")
    for campo, valido in _CAMPOS[pacote["tipo"]].items():
        if campo not in pacote or not valido(pacote[campo]):
            return f"{campo} ausente ou inválido"
    return None
