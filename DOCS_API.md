# Documentação da API de Injeção de Bateladas

Este documento descreve como utilizar a API externa para injetar lotes de arquivos (bateladas), iniciar o processamento automaticamente e consultar o status da execução no Verificador de Petições.

---

## 1. Criar e Iniciar Batelada

**URL:** `POST /api/v1/batch`  
**Content-Type:** `application/json`

Este endpoint realiza duas ações em sequência:
1. Cria uma nova batelada (`batch`) com a lista de arquivos fornecida.
2. Tenta iniciar imediatamente o worker (robô) para processar essa batelada, caso o sistema esteja livre.

### Parâmetros do Corpo (JSON)

| Campo     | Tipo     | Obrigatório | Descrição                                                                 |
|-----------|----------|-------------|---------------------------------------------------------------------------|
| `files`   | Array    | Sim         | Lista de strings contendo os nomes dos arquivos (ex: "proc_123.pdf").     |
| `usuario` | String   | Não         | CPF do usuário Projudi. Se omitido, usa o configurado no sistema.         |
| `mode`    | String   | Não         | Modo do navegador: `"headless"` (padrão, invisível) ou `"visible"`.       |

### Exemplo de Requisição

```json
{
  "files": [
    "5188032.43.2019.8.09.0152_9565_56790_Manifestação.pdf",
    "176359.51.2013.8.09.0152 Certidão optante simples nacional - 2025.pdf"
  ],
  "usuario": "12345678900",
  "mode": "headless"
}
```

### Exemplo de Resposta (Sucesso)

```json
{
  "ok": true,
  "batch_id": "a1b2c3d4",
  "count": 2,
  "worker_started": true,
  "message": "Batch created and worker started"
}
```

- `ok`: Indica sucesso da operação.
- `batch_id`: Identificador único da batelada criada (guarde este ID para consulta).
- `count`: Quantidade de arquivos inseridos com sucesso.
- `worker_started`: `true` se o robô iniciou, `false` se já havia outro robô rodando (neste caso, fica na fila).

---

## 2. Consultar Status da Batelada

**URL:** `GET /api/v1/batch/:batch_id`

Retorna o progresso atual e o status de uma batelada específica.

### Parâmetros da URL

| Parâmetro   | Descrição                                      |
|-------------|------------------------------------------------|
| `:batch_id` | O ID da batelada retornado na criação (ex: `a1b2c3d4`). |

### Exemplo de Resposta

```json
{
  "ok": true,
  "batch_id": "a1b2c3d4",
  "status": "running",
  "created_at": "2023-10-27 10:00:00",
  "finished_at": null,
  "progress": {
    "total": 10,
    "pending": 5,
    "running": 1,
    "done": 4,
    "failed": 0,
    "percentage": 40
  }
}
```

- `status`: Pode ser `queued` (na fila), `starting` (iniciando), `running` (em execução), `done` (finalizado) ou `error`.
- `progress.percentage`: Porcentagem de conclusão (0 a 100).

---

## Exemplos de Uso

### 1. cURL (Linha de Comando)

#### Criar Batelada
```bash
curl -X POST http://localhost:3745/api/v1/batch \
  -H "Content-Type: application/json" \
  -d '{
    "files": ["Processo A.pdf", "Processo B.pdf"],
    "mode": "headless"
  }'
```

#### Consultar Status
```bash
curl http://localhost:3745/api/v1/batch/a1b2c3d4
```

### 2. Python (Script de Automação)

```python
import requests
import time

BASE_URL = "http://localhost:3745/api/v1/batch"

# 1. Criar Batelada
payload = {
    "files": ["Arquivo1.pdf", "Arquivo2.pdf"],
    "mode": "headless"
}
response = requests.post(BASE_URL, json=payload)
data = response.json()
batch_id = data['batch_id']
print(f"Batch criado: {batch_id}. Worker iniciado: {data['worker_started']}")

# 2. Monitorar Progresso
while True:
    res = requests.get(f"{BASE_URL}/{batch_id}")
    status = res.json()
    
    pct = status['progress']['percentage']
    st = status['status']
    print(f"Status: {st} - {pct}% concluído")
    
    if st in ['done', 'error']:
        print("Finalizado!")
        break
        
    time.sleep(5)
```
