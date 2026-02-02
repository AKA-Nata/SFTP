## Visão geral

Este repositório contém 2 scripts de sincronização via SFTP (origem -> destino), com lógica de:
- listar arquivos na origem
- comparar com o destino (por tamanho)
- enviar novos ou substituir divergentes
- registrar tudo em log e enviar o log para uma pasta remota

Scripts:

1) **SFTP_Cliente1.py**
   - Sincroniza **arquivos do dia (D0)** do SFTP **origem** (cliente1) para o SFTP **destino** (Preambulo).
   - Evita reenviar arquivos que já estejam no destino em `uploads` ou `processados`.
   - Critério de comparação: **tamanho do arquivo**.

2) **SFTP_Cliente2.py**
   - Sincroniza **arquivos recentes** (por padrão últimos **2 dias**) do SFTP **origem** (cliente2) para o SFTP **destino** (Preambulo).
   - Critério de comparação: **tamanho do arquivo**.



## Variáveis de ambiente

Os scripts tentam carregar um `.env` local automaticamente.

### SFTP_Cliente1.py

Origem (cliente1):
- `CLIENTE1_SFTP_HOST`
- `CLIENTE1_SFTP_PORT` (ex: `2222`)
- `CLIENTE1_SFTP_USER`
- `CLIENTE1_SFTP_PASS`
- `CLIENTE1_REMOTE_DIR` (ex: `/FTP/Outbound`)

Destino (Preambulo):
- `PREAMBULO_SFTP_HOST`
- `PREAMBULO_SFTP_PORT` (ex: `22`)
- `PREAMBULO_SFTP_USER`
- `PREAMBULO_SFTP_PASS`
- `PREAMBULO_REMOTE_UPLOADS_DIR` (ex: `/preambulo/uploads/`)
- `PREAMBULO_REMOTE_PROCESSADOS_DIR` (ex: `/preambulo/uploads/processados/`)
- `PREAMBULO_REMOTE_LOG_DIR` (ex: `/preambulo/uploads/logs/SCRIPT/cliente1/`)

Segurança (opcional):
- `SFTP_KNOWN_HOSTS` (caminho para `known_hosts`)
- `SFTP_DISABLE_HOSTKEY_CHECK` (`true`/`false`)

### SFTP_Cliente2.py

Origem (cliente2):
- `CLIENTE2_SFTP_HOST`
- `CLIENTE2_SFTP_PORT` (ex: `9022`)
- `CLIENTE2_SFTP_USER`
- `CLIENTE2_SFTP_PASS`
- `CLIENTE2_REMOTE_DIR` (ex: `/Prod_Canais_Fisicos/`)

Destino (Preambulo):
- `PREAMBULO_SFTP_HOST`
- `PREAMBULO_SFTP_PORT`
- `PREAMBULO_SFTP_USER`
- `PREAMBULO_SFTP_PASS`
- `PREAMBULO_REMOTE_DIR` (ex: `/Prod_Canais_Fisicos/`)
- `PREAMBULO_REMOTE_LOG_DIR` (ex: `/Prod_Canais_Fisicos/logs/SCRIPT/`)

Janela de tempo (opcional):
- `DAYS_BACK` (ex: `2`) — padrão 2 dias

Compatibilidade (opcional):
- `KEEP_EXTRA_LOCAL_COPY` (`true`/`false`) — padrão `true`

Segurança (opcional):
- `SFTP_KNOWN_HOSTS`
- `SFTP_DISABLE_HOSTKEY_CHECK`

---

## Dependências

- Python 3.10+
- `pysftp`

Instalação:
```
pip install pysftp
```
