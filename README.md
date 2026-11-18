# Chat com ordenação

Trabalho final de Sistemas Distribuídos de 2026, por Levi, Henri, André e Gabriel.

Implementados (aguardando revisão do grupo): transporte FIFO com retry,
multicast ordenado por Lamport, heartbeat, eleição Bully, bootstrap da visão
inicial, reorganização após a queda de um nó e reentrada por JOIN/SNAPSHOT
(T02–T06), cliente web com reconexão preservando UUID (T07) e painel do cluster
com cenários de concorrência, retry, queda e retorno validados no Compose (T08;
evidências em [docs/evidencias/](docs/evidencias/)).
Para reproduzir e demonstrar, veja [Reprodução para avaliação](#reprodução-para-avaliação) (T09).
Pendente: relatório e apresentação (T10).

## Por onde começar

- [app/](app/): código do nó. `no.py` reúne as regras; `ordenacao.py`, a fila
  do multicast; `reconciliacao.py`, a junção dos estados na troca de visão;
  `transporte.py`, os canais FIFO; `main.py`, as rotas.
- [tests/](tests/): testes unitários, simulação de rede e integração.
- [scripts/validar_compose.py](scripts/validar_compose.py): validação da
  queda do líder e de membro comum no Docker Compose.
- [scripts/validar_reentrada.sh](scripts/validar_reentrada.sh): validação de queda
  e retorno dos três nós, com todos os clientes e testes em contêineres.
- [scripts/validar_retry.sh](scripts/validar_retry.sh): congela o nó 1 por 1 s
  durante envios e confirma o retry do transporte sem duplicar nem reorganizar.
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
nós. Com só um nó vivo, o chat fica pausado até um dos que caíram voltar: os
dois formam maioria e retomam com o histórico do sobrevivente. Um nó que volta depois de cair
solicita entrada ao líder dos sobreviventes. O grupo pausa brevemente, reconcilia
as mensagens em trânsito e transfere o histórico e o relógio ao nó reiniciado.
Só retoma depois de todos confirmarem a instalação. Os UUIDs entregues são
reconstruídos do histórico, permitindo reenvio sem duplicação. O maior ID volta
a ser líder; a época identifica quem propôs a transição, que pode ser outro nó.

### 1. Preparar o ambiente (só na primeira vez)

**Sem instalar Python na máquina**, use Docker para executar toda a suíte:

```sh
docker build --target testes -t chat-aovivo-test .
docker run --rm chat-aovivo-test
```

Isso inclui os testes HTTP/WebSocket com três processos reais dentro do
contêiner. Para testar somente a reentrada:
`docker run --rm chat-aovivo-test python -m pytest tests/test_reentrada.py -q`.
Refaça o build depois de alterar código ou testes. A imagem normal do Compose
continua sendo o estágio `runtime`, sem as dependências de teste.

Como alternativa, para execução local:

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

Resultado atual: **404 testes aprovados** em 18/11, no contêiner de testes (cerca de 35 segundos);
A T06 acrescenta testes de admissão, barreiras, controles atrasados e integração com reinício. Os testes:

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

Religue somente o nó que caiu, usando o mesmo comando do terminal: ele volta com
outra encarnação, sincroniza e é readmitido automaticamente. Aguarde o fim da
transição antes de provocar outra queda. Reiniciar os três nós começa um grupo
novo e vazio, porque o estado é mantido em memória.

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
Para readmitir o nó derrubado: `docker compose start no3`.
Na queda do líder, os sobreviventes passam a `(2, 2)`; no retorno do nó 3,
os três passam a `(3, 2)`, com líder 3. O segundo número identifica o autor
da proposta (nó 2), e não necessariamente o líder da visão instalada.

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

**Validação da reentrada (T06), sem Python no host.** Com as portas 8001–8003
livres, execute no macOS/Linux (ou em um shell compatível):

```sh
sh scripts/validar_reentrada.sh
```

Para testar só o líder: `sh scripts/validar_reentrada.sh 3`.
O roteiro usa o projeto Compose separado `chat-t06`, constrói as imagens e,
para cada nó, envia 45 mensagens concorrentes, executa `kill`, verifica a
retomada dos sobreviventes e envia durante a ausência. Depois executa `start`,
compara os históricos completos dos três nós, reenvia o UUID já confirmado ao
nó reiniciado e testa um novo envio dele e mais 45 mensagens concorrentes.
Os clientes rodam no serviço `testes` de [compose.testes.yaml](compose.testes.yaml).
O roteiro remove seus contêineres, rede e volume temporário ao terminar.
Verificado em 11/11 com Docker/OrbStack: os três cenários passaram; a readmissão
levou cerca de 2–3 segundos na execução observada, com 92 eventos únicos ao final
de cada cenário. Esse tempo é uma observação, não um prazo garantido.
A queda nesse roteiro ocorre após a entrega; a admissão com mensagens em
trânsito entre sobreviventes também é coberta em `tests/test_reentrada.py`.

### Ajustes opcionais

Os tempos podem ser mudados por variáveis de ambiente, em segundos:
`HEARTBEAT_INTERVALO` (padrão 1), `SUSPEITA_TIMEOUT` (3), `ELEICAO_TIMEOUT` (1)
e `COORDENADOR_TIMEOUT` (3).

## Reprodução para avaliação

Roteiro para outra pessoa subir três nós, abrir dois clientes, observar a
ordenação e derrubar o líder, do zero. Só precisa de **Docker** (Docker Desktop
no Windows/macOS, ou Docker Engine com o plugin Compose no Linux) e de um
navegador. Não é preciso instalar Python. Os comandos funcionam iguais no
PowerShell, no Prompt de Comando e no bash. Execute-os na pasta do projeto.

### Passo a passo

**1. Subir os três nós** (a primeira vez baixa a imagem do Python e instala as
dependências, de 20 segundos a 1 minuto conforme a internet; as seguintes,
poucos segundos):

```sh
docker compose up -d --build
```

Confira que os três estão ativos:

```sh
docker compose logs no1 no2 no3 | grep "nó ativo"
```

Devem aparecer três linhas `nó ativo na época (1, 3)`. No Windows sem `grep`,
abra <http://localhost:8001/estado> e confira `"estado":"ativo"`.

**2. Abrir dois clientes**, cada um em um nó e com uma pessoa diferente:

- <http://localhost:8001/?apelido=Ana>
- <http://localhost:8002/?apelido=Beto>

O painel no topo de cada aba mostra o nó, o estado, o **líder (nó 3)**, a
**época `(1, 3)`**, o relógio de Lamport e um cartão por nó. Ele se atualiza a
cada 1,5 segundo, então pode mostrar uma mudança com esse atraso.

**3. Observar a ordenação.** Envie mensagens pelas duas abas, inclusive quase ao
mesmo tempo. As duas abas mostram as mesmas mensagens **na mesma ordem**. Abaixo
de cada mensagem aparecem a posição e o carimbo usado para ordenar, por exemplo
`#3 · relógio (10, nó 2)`.

**4. Derrubar o líder:**

```sh
docker compose stop no3
```

Em 4 a 6 segundos, as duas abas passam por **reorganizando** e voltam a
**ativo**, agora com **líder nó 2** e época **`(2, 2)`**; o cartão do nó 3 fica
como suspeito. O histórico continua igual nas duas abas, e o botão Enviar
volta a funcionar. Envie uma mensagem para confirmar.

**5. Trazer o nó 3 de volta:**

```sh
docker compose start no3
```

Em cerca de 3 a 4 segundos, os três nós ficam ativos na época **`(3, 2)`**, com líder
nó 3. Abra <http://localhost:8003/?apelido=Caio>: o nó 3 recebeu todo o
histórico, inclusive o que foi enviado enquanto estava fora.

**6. Encerrar:**

```sh
docker compose down
```

O estado fica só na memória: subir de novo começa um grupo vazio.

### Problemas comuns

| Sintoma | O que fazer |
| --- | --- |
| `port is already allocated` | Outro programa usa 8001–8003. Feche-o, ou rode `docker compose down` se for uma execução anterior. |
| Abas mostram **desconectado** | Os contêineres não estão de pé: `docker compose ps`. |
| Os nós ficam em `sincronizando` | Os três precisam subir juntos na primeira vez. Rode `docker compose down` e repita o passo 1. |
| Nada muda depois de editar o código | Use sempre `--build` no passo 1. |

Para ver o que cada nó está fazendo: `docker compose logs -f no1` (eleição,
suspeitas, corte, instalação da visão). O JSON completo do estado fica em
`http://localhost:800X/estado`, e o histórico em `http://localhost:800X/historico`.

### Roteiro da demonstração (8 minutos)

Antes de começar: rode o passo 1 com antecedência, deixe as abas de Ana (8001)
e Beto (8002) abertas lado a lado e um terminal na pasta do projeto.

| Tempo | O que mostrar | O que falar |
| --- | --- | --- |
| 0:00–1:00 | Slide de arquitetura | Três nós equivalentes, HTTP entre nós, WebSocket para os clientes, estado em memória. |
| 1:00–1:45 | Painel das duas abas | Nós, líder 3, época `(1, 3)`, relógio de Lamport; a visão inicial é proposta pelo maior ID. |
| 1:45–3:15 | Passo 3: Ana e Beto conversam, com envios quase simultâneos | Multicast com MSG/ACK em canais FIFO; a ordem total vem de (relógio, nó); o carimbo de cada mensagem mostra isso. |
| 3:15–5:15 | Passo 4: `docker compose stop no3` e envio depois da retomada | Heartbeat (suspeita em 3 s), eleição Bully (nó 2), corte da época, reconciliação dos históricos e nova visão `(2, 2)`; o chat pausa durante a troca e não perde mensagens confirmadas. |
| 5:15–6:45 | Passo 5: `docker compose start no3` e aba do Caio (8003) | JOIN/SNAPSHOT: o nó volta, recebe histórico e relógio e entra na época `(3, 2)`; UUIDs já entregues não são duplicados. |
| 6:45–7:30 | Evidências em `docs/evidencias/` | Concorrência (45 mensagens), retry com nó congelado, queda e retorno dos três nós, reconexão do cliente com o mesmo UUID; 404 testes automáticos. |
| 7:30–8:00 | Encerramento | Limitações: estado em memória, sem tolerância a partição (com um só nó vivo o chat pausa: escolhemos consistência, CAP). |

Os passos 1 a 5 foram seguidos em 18/11/2026 no Docker Desktop (Windows), com
os resultados descritos acima: ver [docs/evidencias/t09-roteiro.log](docs/evidencias/t09-roteiro.log).
O ensaio completo da demonstração, com as falas e cronometrado, ainda precisa
ser feito pelo grupo.

### Outras validações

Além do roteiro manual, os scripts de `scripts/` repetem os cenários de forma
automática (ver "Com Docker" acima) e guardam a saída em `docs/evidencias/`.

O relatório final será escrito em TeX e deverá declarar o uso de IA.
