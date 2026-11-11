"""T04: heartbeat, suspeita e Bully, com rede assíncrona em memória."""

import asyncio
import copy
import uuid

from app.config import Config
from app.modelos import Visao
from app.no import ATIVO, REORGANIZANDO, SINCRONIZANDO, No
from tests.simulacao import mensagem

TEMPOS = dict(heartbeat_intervalo=0.05, suspeita_timeout=0.2, eleicao_timeout=0.1, coordenador_timeout=0.3)


class Relogio:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class RedeAssincrona:
    """Envio direto entregue no próximo giro do loop; nós em `mortos` não recebem nem enviam.

    Tipos em `retidos` não são entregues: permite isolar a eleição da reorganização.
    """

    def __init__(self):
        self.nos: dict[int, No] = {}
        self.mortos: set[int] = set()
        self.retidos: set[str] = set()

    def para(self, origem):
        rede = self

        class _Rede:
            def enfileirar(self, destino, pacote):
                rede._entregar(origem, destino, pacote)

            def enviar_direto(self, destino, pacote):
                rede._entregar(origem, destino, pacote)

            def encerrar_epoca(self, epoca):
                pass  # sem filas: cada pacote é entregue no próximo giro do loop

            def tamanhos(self):
                return {}

        return _Rede()

    def _entregar(self, origem, destino, pacote):
        if origem in self.mortos or destino in self.mortos or pacote["tipo"] in self.retidos:
            return
        asyncio.get_running_loop().call_soon(self.nos[destino].receber_pacote, copy.deepcopy(pacote))


def criar(relogio, sem_visao=()):
    rede = RedeAssincrona()
    nos_cfg = {i: f"http://no{i}" for i in (1, 2, 3)}
    visao = Visao((1, 3), 3, frozenset({1, 2, 3}))
    for i in (1, 2, 3):
        no = No(Config(no_id=i, nos=nos_cfg, **TEMPOS), rede.para(i), agora=relogio)
        if i not in sem_visao:
            no.instalar(visao, [], 0)
            no.retomar()
        rede.nos[i] = no
    return rede


async def esperar(condicao, limite=3.0):
    fim = asyncio.get_running_loop().time() + limite
    while not condicao():
        assert asyncio.get_running_loop().time() < fim, "condição não atingida a tempo"
        await asyncio.sleep(0.01)


def test_queda_do_lider_elege_o_maior_vivo_sem_liberar_envios():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio)
        rede.mortos.add(3)
        rede.retidos.add("VIEW_PREPARE")  # só a eleição: a reorganização (T05) não avança

        # Heartbeats de 1 e 2 continuam; o líder 3 fica em silêncio além do timeout (0,2).
        relogio.t = 0.15
        for no in (rede.nos[1], rede.nos[2]):
            no.ciclo()
        await asyncio.sleep(0)  # entrega os heartbeats entre 1 e 2
        relogio.t = 0.3
        rede.nos[1]._detectar_falhas()
        rede.nos[2]._detectar_falhas()

        await esperar(lambda: rede.nos[1].coordenador == 2 and rede.nos[2].coordenador == 2)
        for i in (1, 2):
            assert rede.nos[i].estado == REORGANIZANDO
            assert rede.nos[i].suspeitos == {3}
            resposta = rede.nos[i].aceitar_do_cliente(mensagem(str(uuid.uuid4()), "depois da eleição"))
            assert resposta["tipo"] == "recusada"  # eleição não libera envios
        assert rede.nos[1].visao.epoca == (1, 3)  # a visão só muda na reorganização (T05)
        assert rede.nos[1].fase is None  # sem PREPARE, o nó 1 nem chegou ao corte

    asyncio.run(cenario())


def test_eleicao_iniciada_por_um_so_no_tambem_converge():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio)
        rede.mortos.add(3)
        rede.nos[1]._suspeitar(3)  # só o nó 1 percebeu a queda
        await esperar(lambda: rede.nos[1].coordenador == 2 and rede.nos[2].coordenador == 2)
        assert rede.nos[2].estado == REORGANIZANDO

    asyncio.run(cenario())


def test_no_sincronizando_nao_participa_da_eleicao():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio, sem_visao={3})  # nó 3 vivo, mas reiniciado e sem visão
        assert rede.nos[3].estado == SINCRONIZANDO
        rede.nos[1]._suspeitar(3)
        rede.nos[2]._suspeitar(3)
        await esperar(lambda: rede.nos[1].coordenador == 2 and rede.nos[2].coordenador == 2)
        assert rede.nos[3].coordenador is None

    asyncio.run(cenario())


def test_maior_id_vivo_contesta_coordenador_menor():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio)
        # O nó 1 suspeita do 3 por engano e, sem receber OK a tempo, se anunciaria.
        rede.nos[1]._assumir_coordenacao()
        await esperar(lambda: all(n.coordenador == 3 for n in rede.nos.values()))

    asyncio.run(cenario())


def test_suspeita_de_membro_comum_pausa_sem_eleicao():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio)
        relogio.t = 1.0
        rede.nos[3].ultimo_contato[2] = 1.0
        rede.nos[3]._detectar_falhas()  # nó 1 em silêncio para o líder 3
        assert rede.nos[3].suspeitos == {1}
        assert rede.nos[3].estado == REORGANIZANDO
        assert rede.nos[3].coordenador == 3  # nenhuma eleição: o líder segue vivo

    asyncio.run(cenario())


def test_bootstrap_instala_a_visao_inicial_em_todos():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio, sem_visao={1, 2, 3})
        for _ in range(5):
            for no in rede.nos.values():
                no.ciclo()
            await asyncio.sleep(0.01)
        await esperar(lambda: all(n.estado == ATIVO for n in rede.nos.values()))
        for no in rede.nos.values():
            assert no.visao == Visao((1, 3), 3, frozenset({1, 2, 3}))
        assert rede.nos[3]._transicao is None  # coordenador viu todos ativos

    asyncio.run(cenario())


def test_bootstrap_nao_ocorre_se_algum_par_ja_tem_visao():
    async def cenario():
        relogio = Relogio()
        rede = criar(relogio, sem_visao={3})  # 1 e 2 já operam; 3 reiniciou
        for _ in range(5):
            for no in rede.nos.values():
                no.ciclo()
            await asyncio.sleep(0.01)
        assert rede.nos[3].visao is None and rede.nos[3].estado == SINCRONIZANDO  # aguarda JOIN (T06)

    asyncio.run(cenario())


def test_eleicao_antiga_nao_pausa_visao_que_ja_retomou():
    async def cenario():
        rede = criar(Relogio())
        no = rede.nos[1]
        rede.retidos.add("ELECTION")  # mantém a eleição esperando o timeout de OK
        tarefa = asyncio.create_task(no.iniciar_eleicao())
        await asyncio.sleep(0)  # eleição da época (1,3) entrou na espera
        assert no._eleicao_ativa
        no.instalar(Visao((2, 2), 2, frozenset({1, 2})), [], 0)
        no.retomar()  # corte, instalação e retomada terminaram antes do timeout
        await tarefa
        assert no.estado == ATIVO and no.coordenador == 2
        assert no.suspeitos == set() and no._transicao is None

    asyncio.run(cenario())
