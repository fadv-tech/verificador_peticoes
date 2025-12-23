# Fluxo do Controlador de Robôs (após clicar "Iniciar")

## Visão Geral
- A interface web envia os arquivos para a fila (`/enqueue`) e registra a execução como `queued`.
- Um worker Python é iniciado (manual via botão ou automaticamente pelo scheduler) e processa os itens por batelada.
- A concorrência por batch é limitada no banco via `max_robots`.
- Heartbeat e watchdog garantem recuperação em travamentos e timeouts.

## Ação na UI
- Clique em "Iniciar" aciona `enqueue()` e envia os arquivos:
  - `node_interface/public/app.js:546-578`
- Parâmetros enviados: `files`, `mode` (`headless`/`visible`), `robots`, `usuario`.

## Enfileiramento no servidor
- Endpoint: `POST /enqueue`
  - Cria `execucoes` com `status='queued'`, registra `total_arquivos` e `max_robots`.
  - Insere cada arquivo em `job_items` como `pending`.
  - `node_interface/server.js:680-717`

## Inicialização de robôs (workers)
- Botão "Iniciar worker": `POST /start-worker`
  - Se já existe execução `running`, não cria outro.
  - Caso contrário, inicia um worker Python desacoplado.
  - `node_interface/server.js:760-782`
- Scheduler automático (conservador):
  - A cada 8s, se não há `running`, pega o próximo `queued` e inicia um único worker.
  - `node_interface/server.js:827-839`
- Criação do processo Python:
  - Usa `pythonw`/`py`/`python`/`python3`, `detached`, `windowsHide`, passa `WORKER_ID`.
  - `node_interface/server.js:654-667`

## Ciclo do worker (robô Python)
- Loop principal por batches com itens `pending`:
  - `verificador_peticoes/src/worker.py:37-56`
- Marca execução como `running` e prepara credenciais:
  - `verificador_peticoes/src/worker.py:55-64`
- Configura navegador (modo `headless`/`visible`) e realiza login:
  - `verificador_peticoes/src/worker.py:65-80`
- Processamento de itens com heartbeat e watchdog:
  - Em cada item: atualiza `heartbeat_at` e loga `heartbeat`.
  - Se o `progress` não muda por 10 heartbeats consecutivos, fecha driver, reseta itens presos e volta execução para `queued`.
  - `verificador_peticoes/src/worker.py:81-101`
- Controle de concorrência por item:
  - Só inicia um item se a quantidade `running` < `max_robots`.
  - `verificador_peticoes/src/database.py:552-566`
- Verificação da protocolização, saneamento de data, gravação e progresso:
  - `verificador_peticoes/src/worker.py:102-143`
- Finalização do batch:
  - Se não há `pending/running`, calcula contagem e marca `done`; caso contrário, volta para `queued`.
  - `verificador_peticoes/src/worker.py:144-151`

## Estados e limites (banco)
- `execucoes.status`: `queued` → `running` → `done`/`error`.
- `execucoes.heartbeat_at`: atualizado a cada log/heartbeat do worker.
  - `verificador_peticoes/src/database.py:591-599`
- `job_items.status`: `pending` → `running` → `done`/`failed`.
- Limite de robôs por batch:
  - Campo `execucoes.max_robots` (preenchido pelo `/enqueue`).
  - Gate de início em `tentar_iniciar_item`.
  - `verificador_peticoes/src/database.py:552-566`

## Watchdogs e recuperação
- Loop de heartbeat sem progresso:
  - Reinicia robô, reseta `running` para `pending`, reabre último `failed` e volta execução para `queued`.
  - `verificador_peticoes/src/worker.py:93-101`, `verificador_peticoes/src/database.py:630-646`
- Timeout de 900s sem logs/heartbeat:
  - Marca itens como `failed` e execução como `error`.
  - `node_interface/server.js:784-805`

## Finalização agressiva
- Listar processos de robôs ativos (Windows) e finalizar:
  - `GET /robots`, `POST /robots/kill`, `POST /robots/kill-workers`
  - `node_interface/server.js:598-635`, `561-583`, `585-596`
- Ao finalizar agressivamente:
  - Marca `job_items` pendentes/em execução como `failed`; `execucoes` como `error`.
  - `node_interface/server.js:615-619`

## Observações de concorrência
- A concorrência por batch é controlada no banco via `max_robots`.
- A criação de múltiplos processos pode ocorrer se:
  - O botão "Iniciar worker" for acionado repetidamente muito rápido antes de `status='running'` ser persistido.
  - Houver sobreposição entre o clique manual e o scheduler (a cada 8s).
- Mesmo com múltiplos processos, o gate `tentar_iniciar_item` impede exceder `max_robots` por batch.