"""Fila de pendências do multicast totalmente ordenado (Lamport + ACK de todos).

Uma mensagem só sai da fila quando:
  1. é a primeira pela chave (t, origem), e
  2. todos os membros da visão a reconheceram.
O próprio MSG conta como reconhecimento da origem.

Com canais FIFO entre cada par de nós, quando o ACK de um membro chega,
qualquer mensagem anterior desse membro já chegou; e tudo o que ele enviar
depois terá carimbo maior. Por isso a primeira da fila pode ser entregue.
"""

from .modelos import Chave, Evento


class FilaOrdenada:
    def __init__(self, membros):
        self.membros = frozenset(membros)
        self._eventos: dict[Chave, Evento] = {}
        self._acks: dict[Chave, set[int]] = {}

    def adicionar(self, evento: Evento) -> None:
        self._eventos[evento.chave] = evento
        self.registrar_ack(evento.chave, evento.origem)

    def registrar_ack(self, chave: Chave, membro: int) -> None:
        """ACKs que chegam antes do MSG correspondente ficam guardados."""
        self._acks.setdefault(chave, set()).add(membro)

    def por_uuid(self, uuid: str) -> Evento | None:
        return next((e for e in self._eventos.values() if e.uuid == uuid), None)

    def retirar_prontos(self) -> list[Evento]:
        """Remove e devolve, em ordem, as mensagens que já podem ser entregues."""
        prontos = []
        while self._eventos:
            primeiro = min(self._eventos.values(), key=lambda e: e.ordem)
            if not self.membros <= self._acks.get(primeiro.chave, set()):
                break
            del self._eventos[primeiro.chave]
            self._acks.pop(primeiro.chave, None)
            prontos.append(primeiro)
        return prontos

    def pendentes(self) -> list[Evento]:
        return sorted(self._eventos.values(), key=lambda e: e.ordem)

    def acks_de(self, chave: Chave) -> set[int]:
        return set(self._acks.get(chave, set()))
