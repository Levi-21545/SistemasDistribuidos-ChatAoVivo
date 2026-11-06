# Chat com ordenação

Trabalho final de Sistemas Distribuídos de 2026, por Levi, Henri, André e Gabriel.

Implementados (aguardando revisão do grupo): transporte FIFO com retry,
multicast ordenado por Lamport, heartbeat, eleição Bully e bootstrap da visão
inicial (T02–T04). Pendentes: reorganização após queda, reentrada, painel e
validação no Compose (T05–T10).

## Por onde começar

- [app/](app/): código do nó. `no.py` reúne as regras; `ordenacao.py`, a fila
  do multicast; `transporte.py`, os canais FIFO; `main.py`, as rotas.
- [tests/](tests/): testes unitários, simulação de rede e integração.

- [Documento de protocolo em TeX](docs/protocolo.tex): formatos de mensagens,
  fluxos de comunicação, confirmação e recuperação proposta.
- [Relatório em TeX](docs/relatorio.tex): arquitetura, decisões, algoritmos,
  discussão de CAP e validação planejada.
- [Tarefas em CSV](tarefas.csv): entregas, dependências, marcos e evidências.

Referência de requisitos: enunciado “Trabalho Final – Sistemas Distribuídos 2026”,
fornecido pelo professor. O PDF não está incluído neste checkout.

Os dois arquivos TeX são documentos independentes e podem ser compilados
separadamente em um editor LaTeX. Ambos são esboços. O relatório final terá
4 a 6 páginas, com decisões consolidadas e resultados efetivamente obtidos.

## Acompanhamento

O CSV usa UTF-8, vírgula como separador e uma linha por entrega.
Os estados são `pendente`, `em_andamento`, `em_revisao`, `bloqueada` e `concluida`.
Os campos `participantes` e `dependencias` aceitam vários valores separados por `;`.
Participantes indicam quem está contribuindo, sem atribuição exclusiva.

As quatro frentes organizam o trabalho e não têm donos fixos. Atualizamos o CSV
no próprio repositório. Uma tarefa só fica concluída quando seu critério tiver
evidência; o campo `evidencia` pode apontar para arquivo, commit ou resultado de teste.
Os marcos são as datas originais do professor, incluindo 28/10, já passado.
São referências de planejamento, não registros de conclusão.

As frentes são comunicação e descoberta; coordenação e eleição; ordenação,
replicação e recuperação; cliente, reprodução e experimentos. Implementação,
revisão, testes e documentação podem ser feitos em conjunto.

## Marcos e entrega

| Data | Marco do professor |
| --- | --- |
| 28/10 | Arquitetura, protocolo inicial e dois nós conversando |
| 04/11 | Eleição, heartbeat e relógio lógico |
| 11/11 | Replicação e recuperação de falha |
| 18/11 | Compose completo, testes e ensaio |
| 25/11 | Apresentação e entrega; relatório no Classroom até 19h30 |

O repositório deve ter commits dos quatro integrantes. A demonstração dispõe de
oito minutos, seguidos por quatro de perguntas. Um vídeo de reserva de três a
quatro minutos é opcional. A logística da demonstração ainda será alinhada.

## Execução

Com Docker (não verificado ainda; o ambiente de desenvolvimento não tinha Docker):

```sh
docker compose up --build
```

Clientes em <http://localhost:8001>, <http://localhost:8002> e <http://localhost:8003>.
Estado de cada nó em `/estado` e histórico em `/historico`.
Para derrubar o líder: `docker compose kill no3`.

Sem Docker, com Python 3.13, um terminal por nó:

```sh
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt   # Linux/macOS: .venv/bin/pip

# repetir com NO_ID=2/porta 8002 e NO_ID=3/porta 8003
NO_ID=1 NOS="1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003"   .venv/Scripts/python -m uvicorn app.main:app --port 8001
```

No PowerShell, defina as variáveis com `$env:NO_ID="1"` e `$env:NOS="..."`.
Os três nós precisam estar no ar para o bootstrap da visão inicial.
Tempos opcionais: `HEARTBEAT_INTERVALO`, `SUSPEITA_TIMEOUT`, `ELEICAO_TIMEOUT`,
`COORDENADOR_TIMEOUT` (segundos).

## Testes

```sh
.venv/Scripts/python -m pytest                 # tudo (cerca de 25 s)
.venv/Scripts/python -m pytest -m "not integracao"   # sem subir processos
```

O teste de integração sobe três processos uvicorn nas portas 18001–18003,
envia mensagens concorrentes por WebSocket e derruba o líder.

## Reprodução para avaliação

O roteiro final deve permitir subir três nós, abrir clientes em nós diferentes,
observar a ordenação, derrubar o líder e conferir a recuperação. Hoje, depois
da queda do líder, os sobreviventes elegem o novo coordenador e ficam pausados:
a retomada depende da reorganização (T05).

O relatório final será escrito em TeX e deverá declarar o uso de IA.
