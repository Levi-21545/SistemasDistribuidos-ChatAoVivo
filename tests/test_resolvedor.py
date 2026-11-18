"""Resolvedor de nomes do transporte: uma busca por nó, IP em cache.

Regressão do teste da T08: com o nó 3 parado, cada heartbeat a ele resolvia o
nome de novo (~4 s no DNS do Docker). As buscas presas lotavam o pool de
resolução, e o VIEW_PREPARE do nó 2 para o nó 1 estourava o tempo de conexão.
"""

import asyncio
import socket

from app.transporte import Resolvedor

NOS = {1: "http://no1:8000", 3: "http://no3:8000"}


def info(ip: str):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 8000))]


class DNSFalso:
    """no1 resolve na hora; no3 fica preso até `liberar` (contêiner parado)."""

    def __init__(self):
        self.chamadas: list[str] = []
        self.liberar = asyncio.Event()
        self.ip_no3 = "172.18.0.2"

    async def __call__(self, host, porta, **_):
        self.chamadas.append(host)
        if host == "no3":
            await self.liberar.wait()
            if self.ip_no3 is None:
                raise socket.gaierror(-2, "Name or service not known")
            return info(self.ip_no3)
        return info("172.18.0.3")


def com_dns(teste):
    async def executar():
        dns = DNSFalso()
        asyncio.get_running_loop().getaddrinfo = dns
        resolvedor = Resolvedor(NOS)
        try:
            await teste(resolvedor, dns)
        finally:
            resolvedor.parar()
    asyncio.run(executar())


def test_no_lento_nao_acumula_buscas_nem_atrasa_os_outros():
    async def teste(resolvedor, dns):
        for _ in range(10):  # dez heartbeats ao nó parado enquanto a busca não termina
            assert resolvedor.url(3, "/p") is None
            resolvedor.falhou(3)
        await asyncio.sleep(0)
        assert dns.chamadas.count("no3") == 1
        assert resolvedor.url(1, "/p") is None  # primeira busca do nó 1, sem esperar o nó 3
        await asyncio.sleep(0)
        assert resolvedor.url(1, "/interno/pacote") == "http://172.18.0.3:8000/interno/pacote"
    com_dns(teste)


def test_falha_de_conexao_busca_de_novo_e_mantem_o_ip_anterior_ate_la():
    async def teste(resolvedor, dns):
        dns.liberar.set()
        resolvedor.buscar(3)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert resolvedor.url(3, "/p") == "http://172.18.0.2:8000/p"
        dns.liberar.clear()
        dns.ip_no3 = "172.18.0.9"  # o nó voltou com outro IP
        resolvedor.falhou(3)
        assert resolvedor.url(3, "/p") == "http://172.18.0.2:8000/p"
        dns.liberar.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert resolvedor.url(3, "/p") == "http://172.18.0.9:8000/p"
    com_dns(teste)


def test_nome_nao_resolvido_mantem_o_ip_conhecido():
    async def teste(resolvedor, dns):
        dns.liberar.set()
        resolvedor.buscar(3)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        dns.ip_no3 = None  # contêiner parado: o DNS não conhece mais o nome
        resolvedor.falhou(3)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert resolvedor.url(3, "/p") == "http://172.18.0.2:8000/p"
        assert not resolvedor._buscas
    com_dns(teste)
