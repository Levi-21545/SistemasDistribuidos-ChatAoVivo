# Chat com ordenação

Trabalho final de Sistemas Distribuídos de 2026, por Levi, Henri, André e Gabriel.

Implementados (aguardando revisão do grupo): transporte FIFO com retry,
multicast ordenado por Lamport, heartbeat, eleição Bully, bootstrap da visão
inicial e reorganização após a queda de um nó (T02–T05), validados no Compose
em 06/11. Pendentes: reentrada de um nó reiniciado, painel e ensaio da avaliação
(T06–T10).

## Por onde começar

- [app/](app/): código do nó. `no.py` reúne as regras; `ordenacao.py`, a fila
  do multicast; `reconciliacao.py`, a junção dos estados na troca de visão;
  `transporte.py`, os canais FIFO; `main.py`, as rotas.
- [tests/](tests/): testes unitários, simulação de rede e integração.
- [scripts/validar_compose.py](scripts/validar_compose.py): validação da
  queda do líder e de membro comum no Docker Compose.
- [Documento de protocolo em TeX](docs/protocolo.tex): formatos de mensagens,
  fluxos de comunicação, confirmação e reorganização após queda.
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
diferentes. Se um nó cair, os outros percebem em cerca de 3 segundos, pausam o
chat, juntam o que cada um sabia e instalam uma nova visão só com os dois que
sobraram. Se quem caiu foi o líder (nó 3), elegem antes um novo líder (nó 2).
Em seguida, o chat volta a aceitar mensagens, com o mesmo histórico nos dois
nós. Com só um nó vivo, o chat fica pausado. Um nó que volta depois de cair
ainda não é readmitido; isso é a próxima tarefa (T06).

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

Leva cerca de 20 segundos (10 segundos num Mac em 06/11). O resultado
esperado é `345 passed` (verificado em 06/11, Python 3.13, macOS). Os testes:

- simulam a rede em 200 cenários aleatórios, com pacotes repetidos e fora de
  ordem entre canais, e conferem que os três nós entregam a mesma sequência;
- testam a eleição Bully, a detecção de falha e a criação da visão inicial;
- testam o canal FIFO real com HTTP simulado: repete o mesmo pacote após 500,
  503, falha de conexão, timeout ou resposta perdida, e para sem deixar o
  próximo passar à frente quando o destino recusa o pacote (400);
- conferem que pacotes malformados entre nós recebem 400 sem alterar relógio,
  fila, histórico ou deduplicação;
- simulam a queda do líder e de um membro comum, com mensagens em trânsito,
  controles perdidos ou repetidos, históricos incompatíveis e nó reiniciado, e
  conferem a nova visão, os históricos e o que cada cliente recebe;
- sobem três servidores de verdade (portas 18001–18003), enviam mensagens
  concorrentes, derrubam o líder e conferem a retomada.

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

**Derrube o líder.** No terminal 3, aperte `Ctrl+C`. Em 4 a 5 segundos:

- os terminais 1 e 2 mostram `nó 3 suspeito`, `coordenador eleito: nó 2`,
  `corte da época [1, 3]` e, por fim, `nó ativo na época (2, 2)`;
- a faixa do chat passa por **reorganizando** e volta a **ativo · líder nó 2**,
  época `(2, 2)`;
- o botão Enviar volta a funcionar, e as duas abas continuam com o mesmo
  histórico.

Para derrubar um membro comum, feche o terminal 1 em vez do 3: o líder 3
continua e a nova época é `(2, 3)`. Esse roteiro no navegador não foi refeito
depois da T05; o mesmo comportamento foi verificado no Compose (abaixo).

Para recomeçar, feche todos os nós (`Ctrl+C`) e suba os três de novo.
Religar só o nó que caiu ainda não funciona: ele volta com outra encarnação,
os outros o tratam como falho e ele fica sincronizando, fora do grupo, até a
reentrada (T06).

### Endereços úteis

| Endereço | O que mostra |
| --- | --- |
| `http://localhost:8001/` | Chat |
| `http://localhost:8001/estado` | Estado do nó em JSON: visão, líder, relógio, suspeitos, pendências, `fase` e `transicao` (etapa, membros aguardados e erro) |
| `http://localhost:8001/historico` | Mensagens entregues, na ordem total |

Troque a porta (8001, 8002 ou 8003) para ver os outros nós.

### Com Docker

```sh
docker compose up --build
```

O chat fica em <http://localhost:8001>, <http://localhost:8002> e
<http://localhost:8003>. Para derrubar o líder: `docker compose kill no3`.
Para encerrar e remover os contêineres: `docker compose down`.

Verificado em 06/11 (OrbStack, Docker 29.8.2), com `docker compose build
--no-cache`, `docker compose up -d`, `docker compose kill no3` e
`docker compose down`:

- os três nós ficaram ativos na visão `(1, 3)`, com líder 3;
- três clientes WebSocket, um por nó, enviaram 20 mensagens concorrentes cada;
  os três receberam as mesmas 60 entregas na mesma ordem, e `/historico`
  coincidiu nos três nós;
- o mesmo UUID enviado aos nós 1 e 2 e reenviado ao nó 1 apareceu uma única
  vez em cada histórico, e o reenvio recebeu `confirmada`;
- depois de `kill no3`, os nós 1 e 2 suspeitaram do nó 3, elegeram o nó 2 e
  ficaram `reorganizando` na época `(1, 3)`; um novo envio foi recusado e os
  históricos (61 mensagens) continuaram iguais.

Esse roteiro foi executado por script, não pelo navegador.

**Validação da reorganização (T05).** Com as portas 8001–8003 livres:

```sh
.venv/bin/python scripts/validar_compose.py
```

O script roda dois cenários independentes. Cada um sobe o projeto do zero,
envia 45 mensagens concorrentes por três clientes WebSocket, derruba um nó com
`docker compose kill`, espera a nova visão, envia uma mensagem nova, compara os
históricos e termina com `docker compose down`. Qualquer falha encerra com
código 1. Resultado em três execuções em 06/11, a última depois das correções
da revisão da T05 (OrbStack, Docker 29.8.2):

- queda do líder (nó 3): os nós 1 e 2 ficaram ativos na visão `(2, 2)`, com
  líder 2, de 4 a 5 segundos após o `kill`;
- queda de membro comum (nó 1): os nós 2 e 3 ficaram ativos na visão `(2, 3)`,
  com líder 3, em cerca de 4,6 segundos;
- nos dois, os históricos ficaram iguais e preservados, e a mensagem nova chegou
  aos clientes dos dois sobreviventes.

A queda ocorreu com o tráfego já entregue. A recuperação de mensagens em
trânsito no momento da queda foi testada só na simulação (`tests/`).

### Ajustes opcionais

Os tempos podem ser mudados por variáveis de ambiente, em segundos:
`HEARTBEAT_INTERVALO` (padrão 1), `SUSPEITA_TIMEOUT` (3), `ELEICAO_TIMEOUT` (1)
e `COORDENADOR_TIMEOUT` (3).

## Reprodução para avaliação

O roteiro final deve permitir subir três nós, abrir clientes em nós diferentes,
observar a ordenação, derrubar o líder e conferir a recuperação. Hoje isso está
coberto pelo teste manual acima e por `scripts/validar_compose.py`. Faltam a
readmissão do nó que caiu (T06), o painel e o ensaio cronometrado.

O relatório final será escrito em TeX e deverá declarar o uso de IA.
