## Objetivo
Garantir o servidor Node ativo e iniciar exatamente um worker para a batelada, com verificação de estado e logs.

## Passos
1. Confirmar servidor Node:
   - Se já estiver rodando, manter.
   - Se não, iniciar manualmente.
2. Garantir ambiente limpo de robôs:
   - Verificar processos ativos de `worker.py`.
   - Finalizar qualquer worker remanescente.
3. Iniciar o worker para o batch especificado.
4. Verificar execução:
   - Confirmar 1 processo ativo.
   - Acompanhar status do batch e logs de execução.
5. Ações de contingência:
   - Se houver falha de login/navegador, o sistema reprocessa 1 vez; na 2ª falha marca `failed` e segue.

## Comandos
- Servidor Node:
  - `cd E:\PRODUCAO\Fredson3\node_interface`
  - `npm start`
- Verificar robôs:
  - `powershell -NoProfile -Command "Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:3745/robots' | Select-Object -ExpandProperty Content"`
- Finalizar workers (se houver mais de 1):
  - `powershell -NoProfile -Command "Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:3745/robots/kill-workers' -Method Post | Select-Object -ExpandProperty Content"`
- Iniciar worker (batch `8adf9beb`):
  - `powershell -NoProfile -Command "Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:3745/start-worker?batch=8adf9beb' -Method Post | Select-Object -ExpandProperty Content"`
- Verificar logs do batch:
  - `powershell -NoProfile -Command "Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:3745/jobs/8adf9beb/logs' | Select-Object -ExpandProperty Content"`