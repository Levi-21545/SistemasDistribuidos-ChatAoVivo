"""Estruturas de dados compartilhadas: visão, evento e mensagem do cliente."""

import uuid as uuidlib
from dataclasses import dataclass

Epoca = tuple[int, int]  # (contador, ID do coordenador que a propôs)
Chave = tuple[Epoca, int, int]  # (época, origem, relógio original)

CAMPOS_CLIENTE = ("id", "sala", "autor", "texto")
LIMITE_TEXTO = 2000


@dataclass(frozen=True)
class Visao:
    epoca: Epoca
    lider: int
    membros: frozenset[int]

    def json(self) -> dict:
        return {"epoca": list(self.epoca), "lider": self.lider, "membros": sorted(self.membros)}


@dataclass(frozen=True, eq=False)
class Evento:
    """Mensagem do chat com seu carimbo original (relógio, origem) e época."""

    epoca: Epoca
    relogio: int
    origem: int
    mensagem: dict  # id, sala, autor, texto

    @property
    def chave(self) -> Chave:
        return (self.epoca, self.origem, self.relogio)

    @property
    def ordem(self) -> tuple[int, int]:
        """Chave de ordenação dentro de uma visão: (t, origem)."""
        return (self.relogio, self.origem)

    @property
    def uuid(self) -> str:
        return self.mensagem["id"]

    def json(self) -> dict:
        return {
            "epoca": list(self.epoca),
            "relogio": self.relogio,
            "origem": self.origem,
            "mensagem": dict(self.mensagem),
        }

    @classmethod
    def de_json(cls, dados: dict) -> "Evento":
        mensagem, erro = validar_mensagem(dados["mensagem"])
        if erro:
            raise ValueError(erro)
        return cls(tuple(dados["epoca"]), int(dados["relogio"]), int(dados["origem"]), mensagem)


def validar_mensagem(dados) -> tuple[dict | None, str | None]:
    """Valida a mensagem do cliente. Devolve (mensagem normalizada, None) ou (None, erro)."""
    if not isinstance(dados, dict):
        return None, "a mensagem deve ser um objeto JSON"
    faltando = [c for c in CAMPOS_CLIENTE if not isinstance(dados.get(c), str) or not dados[c].strip()]
    if faltando:
        return None, "campos ausentes ou vazios: " + ", ".join(faltando)
    try:
        ident = str(uuidlib.UUID(dados["id"]))
    except ValueError:
        return None, "id deve ser um UUID"
    if len(dados["texto"]) > LIMITE_TEXTO:
        return None, f"texto acima de {LIMITE_TEXTO} caracteres"
    return {"id": ident, "sala": dados["sala"], "autor": dados["autor"], "texto": dados["texto"]}, None
