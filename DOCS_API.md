# Documentação da API de Injeção de Bateladas

Este documento descreve como utilizar a API externa para injetar lotes de arquivos (bateladas) e iniciar o processamento automaticamente no Verificador de Petições.

## Endpoint Principal

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

### Exemplo de Requisição (JSON)

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
- `batch_id`: Identificador único da batelada criada.
- `count`: Quantidade de arquivos inseridos com sucesso.
- `worker_started`: `true` se o robô iniciou, `false` se já havia outro robô rodando (neste caso, fica na fila).

---

## Exemplos de Uso

### 1. cURL (Linha de Comando)

Substitua `localhost:3745` pelo IP/Porta do servidor se estiver remoto.

```bash
curl -X POST http://localhost:3745/api/v1/batch \
  -H "Content-Type: application/json" \
  -d '{
    "files": [
      "Processo A - 12345.pdf",
      "Processo B - 67890.pdf"
    ],
    "mode": "visible"
  }'
```

### 2. Python (Script de Automação)

```python
import requests
import json

url = "http://localhost:3745/api/v1/batch"
payload = {
    "files": [
        "Arquivo_Teste_01.pdf",
        "Arquivo_Teste_02.pdf"
    ],
    "usuario": "12345678900"  # Opcional se já configurado no painel
}
headers = {"Content-Type": "application/json"}

try:
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        data = response.json()
        print(f"Sucesso! Batch ID: {data['batch_id']}")
        print(f"Status do Worker: {data['message']}")
    else:
        print(f"Erro na requisição: {response.text}")
except Exception as e:
    print(f"Erro de conexão: {e}")
```

## Notas Importantes

1. **Fila de Processamento:** Se já houver uma batelada em execução (`worker_started: false`), a nova batelada ficará com status `queued` (na fila) e será processada automaticamente assim que o robô atual terminar.
2. **Credenciais:** O sistema utiliza as credenciais do Projudi salvas no banco de dados. Se o parâmetro `usuario` for enviado e houver senha salva para ele, o sistema fará a troca automática de contexto.
3. **Validação:** Arquivos inválidos ou vazios na lista são ignorados silenciosamente; o campo `count` indica quantos foram efetivamente aceitos.
