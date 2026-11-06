"""Reconciliação dos estados capturados no corte de uma reorganização (T05).

Cada sobrevivente informa o histórico que já entregou aos seus clientes e as
pendências da visão encerrada. O resultado é o histórico da nova visão:

1. os históricos entregues precisam ser prefixos uns dos outros;
2. o mais longo é a base e é preservado inteiro, com as visões anteriores;
3. as pendências da visão encerrada que não estão na base entram depois dela,
   pela chave original (relógio, origem);
4. uma pendência cujo UUID já apareceu antes na ordem total fica de fora, como
   na entrega normal: vale a primeira ocorrência. Essa duplicata já resolvida pode
   ter chave menor que a da última entrega, porque o nó que avançou a descartou.

Com canais FIFO e ACK de todos, nenhuma pendência nova (de UUID ainda não
ocorrido) pode ter chave menor que a de uma mensagem já entregue na mesma visão.
Se isso ou qualquer conflito de identidade aparecer, nada é reescrito:
ErroReconciliacao mantém os nós pausados.
"""

import logging

from .modelos import Epoca, Evento

log = logging.getLogger(__name__)


class ErroReconciliacao(Exception):
    pass


def reconciliar(estados: dict[int, dict], encerrada: Epoca) -> tuple[list[Evento], int]:
    """estados: {nó: {"historico", "pendentes", "relogio"}} -> (histórico, relógio de referência)."""
    historicos = {i: [Evento.de_json(e) for e in s["historico"]] for i, s in estados.items()}
    base_de = max(sorted(historicos), key=lambda i: len(historicos[i]))
    base = historicos[base_de]
    for i, historico in historicos.items():
        for posicao, (evento, na_base) in enumerate(zip(historico, base), start=1):
            if evento.json() != na_base.json():
                raise ErroReconciliacao(
                    f"históricos incompatíveis: nós {i} e {base_de} divergem na posição {posicao}")

    conhecidos = {e.chave: e for e in base}
    pendentes: dict = {}
    for i, estado in sorted(estados.items()):
        for dados in estado["pendentes"]:
            evento = Evento.de_json(dados)
            if evento.epoca != encerrada:
                raise ErroReconciliacao(f"nó {i} informou pendência da época {list(evento.epoca)}, "
                                        f"fora da visão encerrada {list(encerrada)}")
            igual = conhecidos.get(evento.chave) or pendentes.get(evento.chave)
            if igual is None:
                pendentes[evento.chave] = evento
            elif igual.mensagem != evento.mensagem:
                raise ErroReconciliacao(f"evento (época, origem, relógio) {evento.chave} "
                                        f"informado com conteúdos diferentes")

    ultima = max((e.ordem for e in base if e.epoca == encerrada), default=None)
    primeira = {}  # UUID -> primeira ocorrência na ordem total
    for evento in base:
        primeira.setdefault(evento.uuid, evento)
    sufixo = []
    for evento in sorted(pendentes.values(), key=lambda e: e.ordem):
        anterior = primeira.get(evento.uuid)
        if anterior is not None and (anterior.epoca, anterior.ordem) < (evento.epoca, evento.ordem):
            # Duplicata já resolvida: a primeira ocorrência continua valendo.
            nivel = logging.WARNING if anterior.mensagem != evento.mensagem else logging.INFO
            log.log(nivel, "pendência %s fora do histórico: UUID %s já ocorreu em %s%s", evento.chave,
                    evento.uuid, anterior.chave, " com outro conteúdo" if nivel == logging.WARNING else "")
            continue
        if ultima is not None and evento.ordem < ultima:
            raise ErroReconciliacao(f"pendência {evento.ordem} anterior à última mensagem já entregue "
                                    f"{ultima}; a ordem confirmada não será reescrita")
        primeira[evento.uuid] = evento
        sufixo.append(evento)

    relogio = max([s["relogio"] for s in estados.values()] + [e.relogio for e in pendentes.values()])
    return base + sufixo, relogio
