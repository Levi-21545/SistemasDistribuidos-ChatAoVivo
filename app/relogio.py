"""Relógio lógico de Lamport."""


class RelogioLamport:
    def __init__(self, valor: int = 0):
        self.valor = valor

    def tick(self) -> int:
        """Evento local ou envio: incrementa e devolve o novo valor."""
        self.valor += 1
        return self.valor

    def receber(self, t: int) -> int:
        """Recepção de um carimbo t: L <- max(L, t) + 1."""
        self.valor = max(self.valor, t) + 1
        return self.valor

    def avancar_para(self, t: int) -> None:
        """Usado ao instalar uma visão: o relógio nunca é reduzido."""
        self.valor = max(self.valor, t)
