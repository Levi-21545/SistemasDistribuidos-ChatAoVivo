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

## Como rodar e testar

### O que o sistema faz hoje

Três servidores (nós 1, 2 e 3) rodam ao mesmo tempo. Cada pessoa abre o chat
conectada a um deles, e todos veem as mensagens **na mesma ordem**, mesmo em nós
diferentes. Se o líder (nó 3) cair, os outros percebem em cerca de 3 segundos e
elegem um novo líder (nó 2). Por enquanto, depois da queda, o chat fica pausado:
voltar a aceitar mensagens é a próxima tarefa (T05).

### 1. Preparar o ambiente (só na primeira vez)

Precisa de **Python 3.13** ou mais novo. Docker não é necessário.
Na pasta do projeto:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements-dev.txt
```

No Linux/macOS, troque `.\.venv\Scripts\` por `.venv/bin/` em todos os comandos.

### 2. Testes automáticos

```powershell
.\.venv\Scripts\python -m pytest
```

Leva cerca de 20 segundos. O resultado esperado é `242 passed`. Os testes:

- simulam a rede em 200 cenários aleatórios, com pacotes repetidos e fora de
  ordem entre canais, e conferem que os três nós entregam a mesma sequência;
- testam a eleição Bully, a detecção de falha e a criação da visão inicial;
- sobem três servidores de verdade (portas 18001–18003), enviam mensagens
  concorrentes e derrubam o líder.

Para rodar só os testes rápidos, sem subir servidores:
`.\.venv\Scripts\python -m pytest -m "not integracao"`.

### 3. Teste manual no navegador

**Suba os três nós**, um em cada terminal (no VS Code, botão `+` do terminal):

```powershell
# Terminal 1
$env:NO_ID="1"; $env:NOS="1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003"; .\.venv\Scripts\python -m uvicorn app.main:app --port 8001
```

```powershell
# Terminal 2
$env:NO_ID="2"; $env:NOS="1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003"; .\.venv\Scripts\python -m uvicorn app.main:app --port 8002
```

```powershell
# Terminal 3
$env:NO_ID="3"; $env:NOS="1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003"; .\.venv\Scripts\python -m uvicorn app.main:app --port 8003
```

No Linux/macOS (bash), o equivalente para o nó 1 é:

```sh
NO_ID=1 NOS="1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003" .venv/bin/python -m uvicorn app.main:app --port 8001
```

Em poucos segundos, cada terminal mostra `nó ativo na época (1, 3)`.
**Os três precisam estar no ar** para o chat começar.

**Abra o chat** em duas abas, cada uma em um nó e com uma pessoa diferente:

- <http://localhost:8001/?apelido=Ana>
- <http://localhost:8002/?apelido=Beto>

Troque mensagens pelas duas abas. A ordem é a mesma nas duas. Embaixo de cada
mensagem aparece a posição e o carimbo usado para ordenar, por exemplo
`#3 · relógio (10, nó 2)`. A faixa no topo mostra o nó, o estado, o líder,
a época e o relógio.

**Derrube o líder.** No terminal 3, aperte `Ctrl+C`. Cerca de 3 segundos depois:

- os terminais 1 e 2 mostram `nó 3 suspeito` e `coordenador eleito: nó 2`;
- a faixa do chat muda para **reorganizando · líder nó 2**;
- o botão Enviar fica desabilitado: o chat está pausado de propósito até a
  reorganização (T05).

Para recomeçar, feche todos os nós (`Ctrl+C`) e suba os três de novo.
Religar só o nó que caiu ainda não funciona: isso é a reentrada (T06).

### Endereços úteis

| Endereço | O que mostra |
| --- | --- |
| `http://localhost:8001/` | Chat |
| `http://localhost:8001/estado` | Estado do nó em JSON: visão, líder, relógio, suspeitos, pendências |
| `http://localhost:8001/historico` | Mensagens entregues, na ordem total |

Troque a porta (8001, 8002 ou 8003) para ver os outros nós.

### Com Docker (ainda não testado)

```sh
docker compose up --build
```

O chat fica em <http://localhost:8001>, <http://localhost:8002> e
<http://localhost:8003>. Para derrubar o líder: `docker compose kill no3`.

### Ajustes opcionais

Os tempos podem ser mudados por variáveis de ambiente, em segundos:
`HEARTBEAT_INTERVALO` (padrão 1), `SUSPEITA_TIMEOUT` (3), `ELEICAO_TIMEOUT` (1)
e `COORDENADOR_TIMEOUT` (3).

## Reprodução para avaliação

O roteiro final deve permitir subir três nós, abrir clientes em nós diferentes,
observar a ordenação, derrubar o líder e conferir a recuperação. Hoje falta a
recuperação: depois da queda, os sobreviventes elegem o novo coordenador e
ficam pausados até a reorganização (T05).

O relatório final será escrito em TeX e deverá declarar o uso de IA.
