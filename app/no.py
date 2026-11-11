"""Estado e regras de um nó.

As funções que alteram estado são síncronas: o asyncio executa uma de cada vez,
então a atualização do estado e o enfileiramento de cada multicast ficam
serializados sem travas. Só a eleição (que espera timeouts) é assíncrona.

Estados:
  sincronizando  sem visão instalada ou aguardando VIEW_RESUME; fora da eleição.
  ativo          aceita mensagens de clientes.
  reorganizando  suspeita de falha ou eleição em curso; novos envios pausados.

Fases de uma transição de visão (None fora dela):
  corte      VIEW_PREPARE aceito: estado capturado; MSG/ACK da visão encerrada
             são descartados e não alteram mais nada.
  instalada  VIEW_INSTALL aplicado: nova visão em vigor, mas pausada; o sufixo
             do histórico só é publicado aos clientes no VIEW_RESUME.

A pausa por suspeita não é o corte: até o VIEW_PREPARE, MSG e ACK da visão atual
continuam sendo processados e entregues; só os envios de clientes param.
"""

import asyncio
import logging
import time
import uuid as uuidlib
from collections.abc import Callable

from .config import Config
from .modelos import Evento, Visao, validar_mensagem
from .ordenacao import FilaOrdenada
from .reconciliacao import ErroReconciliacao, reconciliar
from .relogio import RelogioLamport

log = logging.getLogger(__name__)

SINCRONIZANDO = "sincronizando"
ATIVO = "ativo"
REORGANIZANDO = "reorganizando"

CORTE = "corte"
INSTALADA = "instalada"

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
        self.historico: list[Evento] = []  # publicado aos clientes, na ordem total
        self.entregues: dict[str, Evento] = {}
        self.ouvintes: list[Callable[[Evento, int], None]] = []
        self._a_publicar: list[Evento] = []  # sufixo instalado, publicado só na retomada

        # Identidade de pacotes: a encarnação muda a cada reinício do processo,
        # então um nó reiniciado nunca repete um pacote_id antigo.
        self.encarnacao = uuidlib.uuid4().hex[:8]
        self._seq = 0
        self._processados: set[str] = set()

        self.ultimo_contato: dict[int, float] = {}
        self.estado_pares: dict[int, dict] = {}
        self._encarnacoes: dict[int, str] = {}  # última encarnação vista de cada par
        self._encarnacoes_encerradas: set[tuple[int, str]] = set()
        self.suspeitos: set[int] = set()
        self._eleicao_ativa = False
        self._recebeu_ok = False
        self._anuncio = asyncio.Event()

        # Participante: fase e corte da transição em curso.
        self.fase: str | None = None
        self._corte: dict | None = None  # transição aceita e estado capturado nela
        # Coordenador: visão proposta por este nó (bootstrap ou reorganização).
        self._transicao: Visao | None = None
        self._anterior: tuple | None = None  # época encerrada; None no bootstrap
        self._estados: dict[int, dict] = {}  # VIEW_STATE recebidos, inclusive o próprio
        self._prontos: set[int] = set()
        self._instalacao: dict | None = None  # histórico e relógio do VIEW_INSTALL
        self._entrantes: dict[int, str] = {}  # coordenador: ID -> encarnação admitida
        self._pedido_join: dict | None = None  # nó sem visão: líder e época consultados
        self._admissao: dict | None = None  # snapshot instalado, aguardando RESUME
        self.erro_transicao: str | None = None
        self._tarefas: set[asyncio.Task] = set()

        self._tratadores = {
            "MSG": self._receber_msg,
            "ACK": self._receber_ack,
            "HEARTBEAT": self._receber_heartbeat,
            "ELECTION": self._receber_election,
            "OK": self._receber_ok,
            "COORDINATOR": self._receber_coordinator,
            "VIEW_PREPARE": self._receber_view_prepare,
            "VIEW_STATE": self._receber_view_state,
            "VIEW_INSTALL": self._receber_view_install,
            "VIEW_READY": self._receber_view_ready,
            "VIEW_RESUME": self._receber_view_resume,
            "JOIN": self._receber_join,
            "SNAPSHOT": self._receber_snapshot,
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
        partes = pacote["pacote_id"].split("-")
        if len(partes) == 3 and (pacote["origem"], partes[1]) in self._encarnacoes_encerradas:
            return 200, {"descartado": "encarnação encerrada"}
        self._observar_encarnacao(pacote)
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
        if self.fase == CORTE and epoca == self.visao.epoca:
            # Depois do corte, a visão atual está encerrada: nada mais altera a captura.
            return 200, {"descartado": "visão encerrada"}
        if epoca > self.visao.epoca:
            return 503, {"erro": "época ainda não instalada"}
        if self.fase == INSTALADA:
            return 503, {"erro": "visão instalada aguardando VIEW_RESUME"}
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

    def instalar(self, visao: Visao, historico: list[Evento], relogio: int) -> str | None:
        """Instala a visão e fica pausado até retomar(); devolve o motivo se recusar.

        O que este nó já publicou precisa ser prefixo do histórico instalado. O
        restante (sufixo) só vai aos clientes, e a /historico, na retomada.
        """
        publicado = [e.json() for e in self.historico]
        if [e.json() for e in historico[:len(publicado)]] != publicado:
            return "o histórico instalado não preserva o que este nó já entregou"
        self.visao = visao
        self.coordenador = visao.lider
        self.fila = FilaOrdenada(visao.membros)
        self._a_publicar = list(historico[len(publicado):])
        self.relogio.avancar_para(relogio)
        self.suspeitos.clear()
        self.fase = INSTALADA
        log.info("visão instalada: época %s, líder %s, membros %s; %d mensagens a publicar na retomada",
                 visao.epoca, visao.lider, sorted(visao.membros), len(self._a_publicar))
        return None

    def retomar(self) -> None:
        self.estado = ATIVO
        self.fase = None
        self._corte = None
        self._admissao = None
        self._pedido_join = None
        self.erro_transicao = None
        sufixo, self._a_publicar = self._a_publicar, []
        for evento in sufixo:
            self._entregar(evento)  # uma vez, na ordem, nas posições seguintes às já publicadas
        agora = self.agora()
        for membro in self._outros_membros():
            self.ultimo_contato[membro] = agora  # carência para o detector de falhas
        log.info("nó ativo na época %s; %d mensagens publicadas na retomada", self.visao.epoca, len(sufixo))

    def _pausar_aplicacao(self, motivo: str) -> None:
        if self.estado == ATIVO:
            self.estado = REORGANIZANDO
            log.warning("envios pausados: %s", motivo)

    def _registrar_erro(self, erro: str) -> None:
        if erro != self.erro_transicao:
            log.error("%s; envios seguem pausados", erro)
        self.erro_transicao = erro

    # Transições. O coordenador conduz uma proposta por vez e a reenvia a cada ciclo
    # até cada etapa se completar:
    #   bootstrap:      VIEW_INSTALL -> VIEW_READY de todos -> VIEW_RESUME
    #   reorganização:  VIEW_PREPARE -> VIEW_STATE de todos -> reconciliação -> as mesmas
    # Bootstrap: o nó de maior ID configurado propõe a primeira visão quando
    # ouviu todos os nós configurados e nenhum deles tem visão. Um nó que
    # reinicia depois encontra pares com visão e solicita JOIN.
    # Reorganização: o coordenador pausado (líder vivo, ou eleito pelo Bully) propõe
    # a próxima época com os membros não suspeitos; é sempre o maior ID entre eles.

    def _conduzir_transicoes(self) -> None:
        if self._transicao is not None:
            self._avancar_transicao(reenviar=True)
        elif self.visao is None:
            if self.id == max(self.cfg.nos) and self._grupo_sem_visao():
                self._propor_visao_inicial()
            else:
                self._solicitar_entrada()
        else:
            self._propor_reorganizacao()

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
        self._transicao, self._anterior = proposta, None
        self._entrantes = {}
        self._prontos = {self.id}
        self._instalacao = {"historico": [], "relogio": 0}
        self.instalar(proposta, historico=[], relogio=0)
        self._avancar_transicao(reenviar=True)

    def _propor_reorganizacao(self) -> None:
        """Coordenador pausado propõe uma única nova visão com os membros não suspeitos."""
        if self.estado != REORGANIZANDO or self.fase is not None or self.coordenador != self.id:
            return
        membros = self.visao.membros - self.suspeitos
        if self.id != max(membros):
            return  # o Bully dá a coordenação ao maior ID vivo
        if 2 * len(membros) <= len(self.cfg.nos):
            self._registrar_erro(f"sem maioria para retomar: vivos {sorted(membros)} "
                                 f"de {len(self.cfg.nos)} nós configurados")
            return
        anterior = self.visao.epoca
        proposta = Visao((anterior[0] + 1, self.id), self.id, frozenset(membros))
        log.warning("reorganização: propondo a visão %s para encerrar a época %s", proposta.json(), list(anterior))
        self._transicao, self._anterior = proposta, anterior
        self._entrantes = {}
        self._estados, self._prontos, self._instalacao = {}, set(), None
        self.erro_transicao = None
        self._cortar(proposta.epoca, anterior, self.id, proposta.membros)
        self._estados[self.id] = self._corte["captura"]
        self._avancar_transicao(reenviar=True)

    def _cortar(self, transicao, anterior, coordenador: int, membros: frozenset) -> None:
        """Corte definitivo: pausa e captura no mesmo passo síncrono, sem await."""
        self.estado = REORGANIZANDO
        self.fase = CORTE
        self.coordenador = coordenador
        self.suspeitos = set(self.visao.membros - membros)  # a proposta define quem fica de fora
        self.erro_transicao = None
        self._corte = {
            "transicao": tuple(transicao), "anterior": tuple(anterior),
            "coordenador": coordenador, "membros": frozenset(membros),
            "captura": {"historico": [e.json() for e in self.historico],
                        "pendentes": [e.json() for e in self.fila.pendentes()],
                        "relogio": self.relogio.valor},
        }
        self.rede.encerrar_epoca(anterior)  # filas e retries de MSG/ACK da visão encerrada
        captura = self._corte["captura"]
        log.warning("corte da época %s para a transição %s: %d entregues, %d pendentes, relógio %d",
                    list(anterior), list(transicao), len(captura["historico"]), len(captura["pendentes"]),
                    captura["relogio"])

    def _avancar_transicao(self, reenviar: bool) -> None:
        """Coleta (só na reorganização), INSTALL até todos READY, RESUME até todos ativos."""
        proposta = self._transicao
        outros = sorted(proposta.membros - {self.id})
        if self.fase == CORTE and not self._reconciliar_se_completo(reenviar):
            return
        faltam_prontos = sorted(proposta.membros - self._prontos)
        if faltam_prontos:
            if reenviar:
                for membro in faltam_prontos:
                    pacote = self._pacote_install(proposta)
                    if membro in self._entrantes:
                        pacote.update(tipo="SNAPSHOT", encarnacao=self._entrantes[membro])
                    self.rede.enviar_direto(membro, pacote)
            return
        if self.fase == INSTALADA:
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

    def _reconciliar_se_completo(self, reenviar: bool) -> bool:
        """Com o VIEW_STATE de todos, reconcilia e instala localmente; True se instalou."""
        proposta = self._transicao
        # Um nó reiniciado não tem estado a reconciliar. Só os membros da visão
        # encerrada contribuem; o entrante recebe o resultado pela barreira comum.
        faltam = sorted(proposta.membros - set(self._entrantes) - set(self._estados))
        if faltam:
            if reenviar:
                for membro in faltam:
                    self.rede.enviar_direto(membro, self._pacote_prepare(proposta))
            return False
        try:
            historico, relogio = reconciliar(self._estados, self._anterior)
        except ErroReconciliacao as erro:
            self._registrar_erro(f"reconciliação da transição {list(proposta.epoca)} falhou: {erro}")
            return False
        erro = self.instalar(proposta, historico, relogio)
        if erro:
            self._registrar_erro(f"transição {list(proposta.epoca)}: {erro}")
            return False
        self._instalacao = {"historico": [e.json() for e in historico], "relogio": relogio}
        self._prontos = {self.id}
        return True

    def _par_ativo_em(self, par: int, visao: Visao) -> bool:
        estado = self.estado_pares.get(par, {})
        return estado.get("estado") == ATIVO and estado.get("epoca") == list(visao.epoca)

    def _pacote_prepare(self, proposta: Visao) -> dict:
        return self._pacote("VIEW_PREPARE", transicao=list(proposta.epoca), anterior=list(self._anterior),
                            membros=sorted(proposta.membros))

    def _pacote_state(self) -> dict:
        corte = self._corte
        return self._pacote("VIEW_STATE", transicao=list(corte["transicao"]), anterior=list(corte["anterior"]),
                            **corte["captura"])

    def _pacote_install(self, proposta: Visao) -> dict:
        return self._pacote(
            "VIEW_INSTALL",
            transicao=list(proposta.epoca),
            anterior=list(self._anterior) if self._anterior else None,
            lider=proposta.lider,
            membros=sorted(proposta.membros),
            **self._instalacao,
        )

    def _pacote_resume(self, proposta: Visao) -> dict:
        return self._pacote("VIEW_RESUME", transicao=list(proposta.epoca))

    def _receber_view_prepare(self, pacote: dict) -> Resposta:
        transicao, anterior = tuple(pacote["transicao"]), tuple(pacote["anterior"])
        membros, origem = frozenset(pacote["membros"]), pacote["origem"]
        if self._corte is not None:
            if (self._corte["transicao"], self._corte["anterior"], self._corte["coordenador"]) != \
                    (transicao, anterior, origem):
                return 200, {"ignorado": "outra transição já está em curso"}
            # Retransmissão do PREPARE: devolve a mesma captura, sem capturar de novo.
        else:
            motivo = self._prepare_recusado(transicao, anterior, membros, origem)
            if motivo:
                return 200, {"ignorado": motivo}
            self._cortar(transicao, anterior, origem, membros)
        self.rede.enviar_direto(origem, self._pacote_state())
        return 200, {"ok": True}

    def _prepare_recusado(self, transicao, anterior, membros, origem) -> str | None:
        if self.visao is None or self.estado == SINCRONIZANDO or self.fase is not None:
            return "nó fora de uma visão em operação"
        if anterior != self.visao.epoca:
            return "a transição não encerra a visão atual deste nó"
        if self.id not in membros or not membros <= self.cfg.nos.keys():
            return "membros propostos fora dos nós configurados"
        ampliacao = membros > self.visao.membros
        if not membros <= self.visao.membros and not ampliacao:
            return "uma admissão não pode remover membros da visão atual"
        coordenador = self.visao.lider if ampliacao else max(membros)
        if origem != coordenador or transicao[1] != origem or transicao <= anterior:
            return "a proposta precisa vir do coordenador da transição, com época superior"
        if 2 * len(membros) <= len(self.cfg.nos):
            return "proposta sem maioria dos nós configurados"
        return None

    def _receber_view_state(self, pacote: dict) -> Resposta:
        proposta = self._transicao
        if (proposta is None or self._anterior is None or self.fase != CORTE
                or tuple(pacote["transicao"]) != proposta.epoca or tuple(pacote["anterior"]) != self._anterior
                or pacote["origem"] not in proposta.membros or pacote["origem"] in self._entrantes):
            return 200, {"ignorado": "estado de transição desconhecida ou já reconciliada"}
        # O primeiro VIEW_STATE de cada membro vale; repetições trazem a mesma captura.
        self._estados.setdefault(pacote["origem"], {c: pacote[c] for c in ("historico", "pendentes", "relogio")})
        self._avancar_transicao(reenviar=False)
        return 200, {"ok": True}

    def _receber_view_install(self, pacote: dict) -> Resposta:
        proposta = Visao(tuple(pacote["transicao"]), pacote["lider"], frozenset(pacote["membros"]))
        coordenador = self._corte["coordenador"] if self._corte else proposta.lider
        if self.id not in proposta.membros or pacote["origem"] != coordenador:
            return 200, {"ignorado": "nó fora da visão proposta ou origem diferente do líder"}
        if pacote["anterior"] is None:
            motivo = self._instalar_bootstrap(proposta, pacote)
        else:
            motivo = self._instalar_reorganizacao(proposta, tuple(pacote["anterior"]), pacote)
        if motivo:
            return 200, {"ignorado": motivo}
        self.rede.enviar_direto(pacote["origem"], self._pacote("VIEW_READY", transicao=list(proposta.epoca)))
        return 200, {"ok": True}

    def _instalar_bootstrap(self, proposta: Visao, pacote: dict) -> str | None:
        if self.visao is None:
            return self.instalar(proposta, [Evento.de_json(e) for e in pacote["historico"]], pacote["relogio"])
        if self.visao.epoca != proposta.epoca or self.fase != INSTALADA:
            return "nó já possui outra visão ou já retomou esta"
        return None  # repetição: só reenvia o READY

    def _instalar_reorganizacao(self, proposta: Visao, anterior, pacote: dict) -> str | None:
        corte = self._corte
        aceita = corte is not None and (corte["transicao"], corte["anterior"], corte["coordenador"],
                                        corte["membros"]) == (proposta.epoca, anterior, pacote["origem"],
                                                              proposta.membros)
        if not aceita:
            return "transição desconhecida, de outro coordenador ou já concluída"
        if self.fase == INSTALADA:
            return None  # repetição: só reenvia o READY, sem reinstalar
        erro = self.instalar(proposta, [Evento.de_json(e) for e in pacote["historico"]], pacote["relogio"])
        if erro:
            self._registrar_erro(f"VIEW_INSTALL {list(proposta.epoca)} recusado: {erro}")
        return erro

    def _receber_view_ready(self, pacote: dict) -> Resposta:
        proposta = self._transicao
        if (proposta is None or self._instalacao is None or tuple(pacote["transicao"]) != proposta.epoca
                or pacote["origem"] not in proposta.membros
                or (pacote["origem"] in self._entrantes and not self._pacote_da_encarnacao(
                    pacote, self._entrantes[pacote["origem"]]))):
            return 200, {"ignorado": "transição desconhecida"}
        self._prontos.add(pacote["origem"])
        self._avancar_transicao(reenviar=False)
        return 200, {"ok": True}

    def _receber_view_resume(self, pacote: dict) -> Resposta:
        contexto = self._corte or self._admissao
        coordenador = contexto["coordenador"] if contexto else (self.visao.lider if self.visao else None)
        if (self.visao is None or tuple(pacote["transicao"]) != self.visao.epoca or self.fase != INSTALADA
                or pacote["origem"] != coordenador):
            return 200, {"ignorado": "retomada desconhecida ou já aplicada"}
        self.retomar()
        return 200, {"ok": True}

    # ------------------------------------------------------------------ reentrada

    def _solicitar_entrada(self) -> None:
        """Sem visão, consulta um líder ativo observado por heartbeat recente."""
        if self.fase is not None:
            return
        for par, estado in sorted(self.estado_pares.items()):
            if (estado["estado"] != ATIVO or estado["coordenador"] != par
                    or estado["epoca"] is None
                    or self.agora() - self.ultimo_contato[par] > self.cfg.suspeita_timeout):
                continue
            self._pedido_join = {"coordenador": par, "anterior": tuple(estado["epoca"])}
            self.rede.enviar_direto(par, self._pacote("JOIN", anterior=estado["epoca"],
                                                      encarnacao=self.encarnacao))
            return

    def _receber_join(self, pacote: dict) -> Resposta:
        """O líder atual conduz a ampliação; o maior ID será líder da nova visão."""
        origem = pacote["origem"]
        if (self.visao is None or self.estado != ATIVO or self.fase is not None
                or self._transicao is not None or self.coordenador != self.id
                or origem in self.visao.membros or pacote["epoca"] is not None
                or tuple(pacote["anterior"]) != self.visao.epoca
                or pacote["encarnacao"] != self._encarnacoes.get(origem)
                or not self._pacote_da_encarnacao(pacote, pacote["encarnacao"])):
            return 200, {"ignorado": "JOIN fora de uma visão estável ou de outra encarnação"}
        membros = self.visao.membros | {origem}
        if 2 * len(self.visao.membros) <= len(self.cfg.nos):
            return 200, {"ignorado": "admissão exige maioria dos sobreviventes"}
        anterior = self.visao.epoca
        proposta = Visao((anterior[0] + 1, self.id), max(membros), membros)
        self._transicao, self._anterior = proposta, anterior
        self._entrantes = {origem: pacote["encarnacao"]}
        self._estados, self._prontos, self._instalacao = {}, set(), None
        log.info("reentrada do nó %s: propondo %s", origem, proposta.json())
        self._cortar(proposta.epoca, anterior, self.id, membros)
        self._estados[self.id] = self._corte["captura"]
        self._avancar_transicao(reenviar=True)
        return 200, {"ok": True}

    @staticmethod
    def _pacote_da_encarnacao(pacote: dict, encarnacao: str) -> bool:
        partes = pacote["pacote_id"].split("-")
        return len(partes) == 3 and partes[:2] == [str(pacote["origem"]), encarnacao]

    def _receber_snapshot(self, pacote: dict) -> Resposta:
        proposta = Visao(tuple(pacote["transicao"]), pacote["lider"], frozenset(pacote["membros"]))
        contexto = {"coordenador": pacote["origem"], "anterior": tuple(pacote["anterior"])}
        if (pacote["encarnacao"] != self.encarnacao or self._pedido_join != contexto
                or self.id not in proposta.membros or pacote["origem"] not in proposta.membros
                or not proposta.membros <= self.cfg.nos.keys()
                or proposta.lider != max(proposta.membros) or proposta.epoca[1] != pacote["origem"]
                or proposta.epoca <= contexto["anterior"]
                or 2 * len(proposta.membros - {self.id}) <= len(self.cfg.nos)):
            return 200, {"ignorado": "snapshot sem JOIN correspondente"}
        if self.visao is not None:
            if self._admissao != contexto or self.visao != proposta or self.fase != INSTALADA:
                return 200, {"ignorado": "snapshot de outra transição ou já retomado"}
        else:
            historico = [Evento.de_json(e) for e in pacote["historico"]]
            # Valida o estado completo antes de instalar: não se aceita UUID repetido,
            # evento de época futura nem relógio abaixo do estado transferido.
            if (len({e.uuid for e in historico}) != len(historico)
                    or any(e.epoca > contexto["anterior"] or e.relogio > pacote["relogio"]
                           for e in historico)):
                return 200, {"ignorado": "snapshot com histórico ou relógio incompatível"}
            erro = self.instalar(proposta, historico, pacote["relogio"])
            if erro:
                return 200, {"ignorado": erro}
            self._admissao = contexto
        self.rede.enviar_direto(pacote["origem"], self._pacote("VIEW_READY", transicao=list(proposta.epoca)))
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
        self._conduzir_transicoes()

    def _pacote_heartbeat(self) -> dict:
        return self._pacote("HEARTBEAT", estado=self.estado, coordenador=self.coordenador)

    def _receber_heartbeat(self, pacote: dict) -> Resposta:
        origem = pacote["origem"]
        self.ultimo_contato[origem] = self.agora()
        self.estado_pares[origem] = {"estado": pacote["estado"], "epoca": pacote["epoca"],
                                     "coordenador": pacote.get("coordenador")}
        return 200, {"ok": True}

    def _observar_encarnacao(self, pacote: dict) -> None:
        """Um membro em operação que aparece com outra encarnação reiniciou e perdeu o estado.

        A encarnação vem do pacote_id (<origem>-<encarnação>-<sequência>) e é registrada
        a partir de qualquer pacote, inclusive o VIEW_INSTALL do bootstrap. Pacotes
        normais ou atrasados da mesma encarnação nunca parecem um reinício. O nó
        reiniciado conta como falha e só volta por JOIN/SNAPSHOT (T06).
        """
        origem = pacote["origem"]
        partes = pacote["pacote_id"].split("-")
        if len(partes) != 3 or partes[0] != str(origem):
            return
        anterior, self._encarnacoes[origem] = self._encarnacoes.get(origem), partes[1]
        if anterior is not None and anterior != partes[1]:
            self._encarnacoes_encerradas.add((origem, anterior))
        if anterior is not None and anterior != partes[1] and self._membro_em_operacao(origem):
            self._suspeitar(origem, motivo=f"reiniciou: encarnação {anterior} -> {partes[1]}")

    def _membro_em_operacao(self, par: int) -> bool:
        return (self.visao is not None and self.estado != SINCRONIZANDO and par in self.visao.membros
                and par not in self.suspeitos)

    def _detectar_falhas(self) -> None:
        if self.visao is None or self.estado == SINCRONIZANDO:
            return
        agora = self.agora()
        for membro in self._outros_membros():
            silencio = agora - self.ultimo_contato.get(membro, agora)
            if membro not in self.suspeitos and silencio > self.cfg.suspeita_timeout:
                self._suspeitar(membro, silencio)

    def _suspeitar(self, membro: int, silencio: float = 0.0, motivo: str | None = None) -> None:
        self.suspeitos.add(membro)
        log.warning("nó %s suspeito: %s", membro, motivo or f"sem heartbeat há {silencio:.1f}s")
        self._pausar_aplicacao(f"suspeita do nó {membro}")
        if self.fase is not None:
            return  # nova falha durante a transição: fora do cenário suportado
        if membro == self.coordenador:
            self._agendar(self.iniciar_eleicao())
        elif self.coordenador == self.id:
            self._propor_reorganizacao()  # queda de membro comum: o líder coordena

    # ------------------------------------------------------------------ Bully

    def _participa_da_eleicao(self) -> bool:
        # Durante uma transição (corte ou instalação), ninguém vota nem se anuncia.
        return self.visao is not None and self.estado != SINCRONIZANDO and self.fase is None

    async def iniciar_eleicao(self) -> None:
        if self._eleicao_ativa or not self._participa_da_eleicao():
            return
        self._eleicao_ativa = True
        epoca = self.visao.epoca
        try:
            while self._participa_da_eleicao() and self.visao.epoca == epoca:
                self._recebeu_ok = False
                self._anuncio.clear()
                maiores = [m for m in self._outros_membros() if m > self.id and m not in self.suspeitos]
                log.info("eleição iniciada; ELECTION para %s", maiores)
                for destino in maiores:
                    self.rede.enviar_direto(destino, self._pacote("ELECTION"))
                if maiores:
                    await asyncio.sleep(self.cfg.eleicao_timeout)
                if not self._participa_da_eleicao() or self.visao.epoca != epoca:
                    return  # a transição pode até já ter retomado durante a espera
                if not self._recebeu_ok:
                    # Bully: os maiores que não responderam são tratados como falhos.
                    if maiores:
                        self.suspeitos.update(maiores)
                        log.warning("nós %s suspeitos: não responderam ao ELECTION", maiores)
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
        self._propor_reorganizacao()

    def _receber_election(self, pacote: dict) -> Resposta:
        # Nós sincronizando (ou de outra época) ficam fora: não respondem OK.
        if self.visao is None or self.estado == SINCRONIZANDO or not self._mesma_epoca(pacote):
            return 200, {"ignorado": "fora da eleição"}
        if pacote["origem"] < self.id:
            # Vivo e maior: responde OK mesmo durante uma transição, para o menor não
            # concluir que caiu. Só inicia a própria eleição fora da transição.
            self.rede.enviar_direto(pacote["origem"], self._pacote("OK"))
            if self._participa_da_eleicao():
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
            "fase": self.fase,
            "transicao": self._diagnostico_transicao(),
        }

    def _diagnostico_transicao(self) -> dict | None:
        """Fase, membros aguardados e erro da transição em curso, para /estado."""
        if self._transicao is not None:
            proposta = self._transicao
            if self.fase == CORTE:
                etapa, aguardando = "coleta", proposta.membros - set(self._entrantes) - set(self._estados)
            elif proposta.membros - self._prontos:
                etapa, aguardando = "instalacao", proposta.membros - self._prontos
            else:
                etapa = "retomada"
                aguardando = {m for m in proposta.membros - {self.id} if not self._par_ativo_em(m, proposta)}
            return {"papel": "coordenador", "epoca": list(proposta.epoca),
                    "anterior": list(self._anterior) if self._anterior else None,
                    "membros": sorted(proposta.membros), "etapa": etapa, "aguardando": sorted(aguardando),
                    "erro": self.erro_transicao}
        if self._corte is not None:
            corte = self._corte
            return {"papel": "participante", "epoca": list(corte["transicao"]), "anterior": list(corte["anterior"]),
                    "membros": sorted(corte["membros"]), "etapa": self.fase, "aguardando": [corte["coordenador"]],
                    "erro": self.erro_transicao}
        return {"erro": self.erro_transicao} if self.erro_transicao else None


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
    "VIEW_PREPARE": {"transicao": _epoca, "anterior": _epoca, "membros": _lista(_inteiro)},
    "VIEW_STATE": {"transicao": _epoca, "anterior": _epoca, "historico": _lista(_evento_json),
                   "pendentes": _lista(_evento_json), "relogio": _inteiro},
    "VIEW_INSTALL": {"transicao": _epoca, "anterior": lambda v: v is None or _epoca(v), "lider": _inteiro,
                     "membros": _lista(_inteiro), "historico": _lista(_evento_json), "relogio": _inteiro},
    "VIEW_READY": {"transicao": _epoca},
    "VIEW_RESUME": {"transicao": _epoca},
    "JOIN": {"anterior": _epoca, "encarnacao": lambda v: isinstance(v, str) and bool(v)},
    "SNAPSHOT": {"transicao": _epoca, "anterior": _epoca, "lider": _inteiro,
                 "membros": _lista(_inteiro), "historico": _lista(_evento_json), "relogio": _inteiro,
                 "encarnacao": lambda v: isinstance(v, str) and bool(v)},
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
