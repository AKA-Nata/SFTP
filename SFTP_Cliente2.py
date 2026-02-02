# SFTP_Cliente2.py
"""
SFTP_Cliente2.py — Sincronização de arquivos recentes cliente2 -> Preambulo

O que este script faz
1) Define uma janela de tempo (por padrão, últimos 2 dias via DAYS_BACK).
   Isso reduz volume e evita varrer conteúdo muito antigo.

2) Conecta no SFTP de destino (Preambulo) e lista arquivos recentes em PREAMBULO_REMOTE_DIR.

3) Conecta no SFTP de origem (cliente2) e lista arquivos recentes em CLIENTE2_REMOTE_DIR.

4) Para cada arquivo recente na origem:
   - Se já existir no destino e o tamanho for igual: pula.
   - Se existir com tamanho diferente: remove e reenviará.
   - Se não existir: envia como novo.

5) Download origem -> pasta temporária local (CARGAS/cliente2/...).
   Opcionalmente, mantém uma cópia adicional local (KEEP_EXTRA_LOCAL_COPY=true/false) por compatibilidade.

6) Upload para o destino, criando diretórios remotos.

7) Envia o log para PREAMBULO_REMOTE_LOG_DIR.

Configuração
- Configure via variáveis de ambiente e/ou arquivo .env.
- Para maior segurança, use host key checking com known_hosts.

Requisitos
- Python 3.10+
- pysftp
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

import pysftp


def load_env_file(env_path: Path) -> None:
    """Carrega um arquivo .env simples (KEY=VALUE) para os.environ (sem sobrescrever valores já setados)."""
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def getenv_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Variável obrigatória ausente: {name}")
    return v


def getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Variável inválida (esperado inteiro): {name}={raw!r}")


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on")


def get_base_dir() -> Path:
    """
    Compatível com execução via .py e empacotamento (PyInstaller).
    - Se estiver empacotado, tenta usar o diretório do executável.
    - Caso contrário, usa o diretório do arquivo .py.
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


@dataclass(frozen=True)
class SFTPConfig:
    host: str
    port: int
    username: str
    password: str
    remote_dir: str


@dataclass(frozen=True)
class Cliente2JobConfig:
    origem: SFTPConfig
    destino: SFTPConfig

    destino_log_dir: str
    days_back: int

    base_dir: Path
    temp_dir: Path
    log_path: Path
    log_filename: str

    disable_hostkey_check: bool
    known_hosts_path: str | None

    keep_extra_local_copy: bool


class StatusContador:
    def __init__(self) -> None:
        self.novos = 0
        self.iguais = 0
        self.reenviados = 0
        self.erros_download = 0
        self.erros_upload = 0
        self.erros_remocao = 0

    def log_resumo(self) -> None:
        logging.info("--- Resumo Final ---")
        logging.info("Arquivos novos enviados: %s", self.novos)
        logging.info("Arquivos iguais pulados: %s", self.iguais)
        logging.info("Arquivos reenviados (substituídos): %s", self.reenviados)
        logging.info("Erros ao baixar: %s", self.erros_download)
        logging.info("Erros ao enviar: %s", self.erros_upload)
        logging.info("Erros ao remover: %s", self.erros_remocao)


def build_cnopts(disable_hostkey_check: bool, known_hosts_path: str | None) -> pysftp.CnOpts:
    cnopts = pysftp.CnOpts()
    if disable_hostkey_check:
        cnopts.hostkeys = None
        return cnopts

    if known_hosts_path:
        p = Path(known_hosts_path)
        if not p.exists():
            raise RuntimeError(f"SFTP_KNOWN_HOSTS não encontrado: {known_hosts_path}")
        cnopts.hostkeys.load(str(p))
    return cnopts


def configurar_logs(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(str(log_path), encoding="utf-8"),
            # logging.StreamHandler(),  # habilite se quiser ver no console
        ],
    )
    logging.getLogger("paramiko").setLevel(logging.WARNING)


def listar_arquivos_sftp_recursivo(
    sftp: pysftp.Connection,
    remote_dir: str,
    *,
    nivel: int = 0,
    filtro_data_min: datetime.date | None = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Lista arquivos recursivamente.

    - Se `filtro_data_min` for informado:
      * inclui arquivos com data_modificacao >= filtro_data_min
      * pode pular pastas antigas no nível 0 (heurística simples)
    """
    arquivos: Dict[str, Dict[str, Any]] = {}
    try:
        entries = sftp.listdir_attr(remote_dir)
        for entry in entries:
            nome = entry.filename
            caminho_remoto = f"{remote_dir.rstrip('/')}/{nome}".replace("//", "/")
            is_dir = entry.longname.startswith("d")
            data_mod = datetime.fromtimestamp(entry.st_mtime).date()

            if is_dir:
                if nivel == 0 and filtro_data_min and data_mod < filtro_data_min:
                    logging.info("Pulando pasta antiga (nível 0): %s", caminho_remoto)
                    continue
                arquivos.update(
                    listar_arquivos_sftp_recursivo(
                        sftp,
                        caminho_remoto,
                        nivel=nivel + 1,
                        filtro_data_min=filtro_data_min,
                    )
                )
            else:
                if not filtro_data_min or data_mod >= filtro_data_min:
                    arquivos[caminho_remoto] = {"tamanho": entry.st_size, "data": data_mod}
    except Exception as e:
        logging.error("Erro ao listar %s: %s", remote_dir, e)

    return arquivos


def ensure_remote_dirs(sftp: pysftp.Connection, remote_dir: str) -> None:
    remote_dir = remote_dir.replace("//", "/")
    if sftp.exists(remote_dir):
        return
    parts = remote_dir.strip("/").split("/")
    path = ""
    for part in parts:
        path += "/" + part
        if not sftp.exists(path):
            sftp.mkdir(path)


def load_job_config() -> Cliente2JobConfig:
    base_dir = get_base_dir()
    load_env_file(base_dir / ".env")

    dt_exec = datetime.now().strftime("%Y%m%d_%H%M")
    log_filename = f"log_execucao_cliente2_{dt_exec}.log"

    temp_dir = base_dir / "CARGAS" / "cliente2"
    log_path = (base_dir / "logs" / "cliente2" / log_filename)

    days_back = getenv_int("DAYS_BACK", 2)

    destino = SFTPConfig(
        host=getenv_required("PREAMBULO_SFTP_HOST"),
        port=getenv_int("PREAMBULO_SFTP_PORT", 22),
        username=getenv_required("PREAMBULO_SFTP_USER"),
        password=getenv_required("PREAMBULO_SFTP_PASS"),
        remote_dir=getenv_required("PREAMBULO_REMOTE_DIR"),
    )

    origem = SFTPConfig(
        host=getenv_required("CLIENTE2_SFTP_HOST"),
        port=getenv_int("CLIENTE2_SFTP_PORT", 9022),
        username=getenv_required("CLIENTE2_SFTP_USER"),
        password=getenv_required("CLIENTE2_SFTP_PASS"),
        remote_dir=getenv_required("CLIENTE2_REMOTE_DIR"),
    )

    destino_log_dir = getenv_required("PREAMBULO_REMOTE_LOG_DIR")

    disable_hostkey_check = getenv_bool("SFTP_DISABLE_HOSTKEY_CHECK", default=False)
    known_hosts_path = os.getenv("SFTP_KNOWN_HOSTS", "").strip() or None

    keep_extra_local_copy = getenv_bool("KEEP_EXTRA_LOCAL_COPY", default=True)

    return Cliente2JobConfig(
        origem=origem,
        destino=destino,
        destino_log_dir=destino_log_dir,
        days_back=days_back,
        base_dir=base_dir,
        temp_dir=temp_dir,
        log_path=log_path,
        log_filename=log_filename,
        disable_hostkey_check=disable_hostkey_check,
        known_hosts_path=known_hosts_path,
        keep_extra_local_copy=keep_extra_local_copy,
    )


def main() -> None:
    cfg = load_job_config()
    configurar_logs(cfg.log_path)

    status = StatusContador()
    cnopts = build_cnopts(cfg.disable_hostkey_check, cfg.known_hosts_path)
    data_limite = (datetime.today() - timedelta(days=cfg.days_back)).date()

    logging.info("Conectando ao SFTP de destino para listar arquivos recentes...")
    with pysftp.Connection(
        cfg.destino.host,
        username=cfg.destino.username,
        password=cfg.destino.password,
        port=cfg.destino.port,
        cnopts=cnopts,
    ) as sftp_pre:
        arquivos_destino = listar_arquivos_sftp_recursivo(sftp_pre, cfg.destino.remote_dir, filtro_data_min=data_limite)

    logging.info("Arquivos recentes no destino: %s", len(arquivos_destino))

    logging.info("Conectando ao SFTP de origem para verificar arquivos recentes...")
    with pysftp.Connection(
        cfg.origem.host,
        username=cfg.origem.username,
        password=cfg.origem.password,
        port=cfg.origem.port,
        cnopts=cnopts,
    ) as sftp_rec:
        arquivos_origem = listar_arquivos_sftp_recursivo(sftp_rec, cfg.origem.remote_dir, filtro_data_min=data_limite)
        logging.info("Arquivos recentes na origem: %s", len(arquivos_origem))

        for caminho, dados in arquivos_origem.items():
            rel_path = os.path.relpath(caminho, cfg.origem.remote_dir).replace("\\", "/")
            caminho_dest = f"{cfg.destino.remote_dir.rstrip('/')}/{rel_path}".replace("//", "/")

            if caminho_dest in arquivos_destino:
                tam_pre = arquivos_destino[caminho_dest]["tamanho"]
                if tam_pre == dados["tamanho"]:
                    logging.info("Igual: %s – pulando.", rel_path)
                    status.iguais += 1
                    continue
                else:
                    logging.info("Tamanho diferente: %s – removendo e reenviando.", rel_path)
                    try:
                        with pysftp.Connection(
                            cfg.destino.host,
                            username=cfg.destino.username,
                            password=cfg.destino.password,
                            port=cfg.destino.port,
                            cnopts=cnopts,
                        ) as sftp_rm:
                            if sftp_rm.exists(caminho_dest):
                                sftp_rm.remove(caminho_dest)
                                logging.info("Arquivo antigo removido: %s", caminho_dest)
                    except Exception as e:
                        logging.error("Erro ao remover %s: %s", caminho_dest, e)
                        status.erros_remocao += 1
                    status.reenviados += 1
            else:
                logging.info("Novo arquivo: %s – enviando.", rel_path)
                status.novos += 1

            # Download
            try:
                local_temp = cfg.temp_dir / rel_path
                local_temp.parent.mkdir(parents=True, exist_ok=True)
                sftp_rec.get(caminho, str(local_temp))

                if cfg.keep_extra_local_copy:
                    local_copy = cfg.base_dir / rel_path
                    local_copy.parent.mkdir(parents=True, exist_ok=True)
                    sftp_rec.get(caminho, str(local_copy))
            except Exception as e:
                logging.error("Erro ao baixar %s: %s", caminho, e)
                status.erros_download += 1
                continue

            # Upload
            try:
                with pysftp.Connection(
                    cfg.destino.host,
                    username=cfg.destino.username,
                    password=cfg.destino.password,
                    port=cfg.destino.port,
                    cnopts=cnopts,
                ) as sftp_envio:
                    pasta_dest = os.path.dirname(caminho_dest)
                    ensure_remote_dirs(sftp_envio, pasta_dest)
                    sftp_envio.put(str(local_temp), caminho_dest)
                    logging.info("Arquivo enviado: %s", caminho_dest)
            except Exception as e:
                logging.error("Erro ao enviar %s: %s", rel_path, e)
                status.erros_upload += 1

    status.log_resumo()

    # Upload do log
    try:
        with pysftp.Connection(
            cfg.destino.host,
            username=cfg.destino.username,
            password=cfg.destino.password,
            port=cfg.destino.port,
            cnopts=cnopts,
        ) as sftp_log:
            ensure_remote_dirs(sftp_log, cfg.destino_log_dir)
            sftp_log.chdir(cfg.destino_log_dir)
            sftp_log.put(str(cfg.log_path), cfg.log_filename)
            logging.info("Log enviado para o destino em: %s", cfg.destino_log_dir)
    except Exception as e:
        logging.error("Erro ao enviar log: %s", e)


if __name__ == "__main__":
    main()
