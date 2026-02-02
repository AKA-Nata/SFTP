# SFTP_Cliente1.py
"""
SFTP_Cliente1.py — Sincronização diária (D0) cliente1 -> Preambulo

O que este script faz
1) Conecta no SFTP de DESTINO (Preambulo) e lista arquivos em:
   - uploads (PREAMBULO_REMOTE_UPLOADS_DIR)
   - processados (PREAMBULO_REMOTE_PROCESSADOS_DIR)
   Isso evita reenvio caso o arquivo já esteja no destino (inclusive já "processado").

2) Conecta no SFTP de ORIGEM (cliente1) e lista arquivos de HOJE (D0) dentro de CLIENTE1_REMOTE_DIR,
   percorrendo subpastas recursivamente.

3) Para cada arquivo de HOJE na origem:
   - Calcula o caminho relativo em relação a CLIENTE1_REMOTE_DIR.
   - Verifica se o mesmo relativo existe no destino (uploads ou processados).
     * Se existir e o tamanho for igual: pula.
     * Se existir e tamanho for diferente: tenta remover do uploads (se existir) e reenviará.
     * Se não existir: envia como novo.

4) Fluxo de transferência:
   - Download origem -> pasta local temporária (CARGAS/cliente1/...)
   - Upload para destino, criando diretórios remotos se necessário.

5) Gera log local e envia o log para PREAMBULO_REMOTE_LOG_DIR ao final.

Configuração
- Configure via variáveis de ambiente e/ou arquivo .env (carregado automaticamente se existir).
- Para maior segurança, use host key checking (SFTP_KNOWN_HOSTS).
  Se necessário, é possível desativar host key check com SFTP_DISABLE_HOSTKEY_CHECK=true (não recomendado).

Requisitos
- Python 3.10+
- pysftp
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True)
class SFTPConfig:
    host: str
    port: int
    username: str
    password: str
    remote_dir: str


@dataclass(frozen=True)
class cliente1JobConfig:
    origem: SFTPConfig
    destino: SFTPConfig

    destino_uploads_dir: str
    destino_processados_dir: str
    destino_log_dir: str

    base_dir: Path
    temp_dir: Path
    log_path: Path
    log_filename: str

    disable_hostkey_check: bool
    known_hosts_path: str | None


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
            logging.StreamHandler(),
        ],
    )


def listar_arquivos_sftp_recursivo(
    sftp: pysftp.Connection,
    remote_dir: str,
    *,
    filtro_data: datetime.date | None = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Lista arquivos recursivamente a partir de `remote_dir`.

    Retorna dict: { caminho_remoto: {'tamanho': int, 'data': date} }

    Se `filtro_data` for informado, inclui apenas arquivos cuja data de modificação == filtro_data.
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
                arquivos.update(listar_arquivos_sftp_recursivo(sftp, caminho_remoto, filtro_data=filtro_data))
            else:
                if filtro_data is None or data_mod == filtro_data:
                    arquivos[caminho_remoto] = {"tamanho": entry.st_size, "data": data_mod}
    except Exception as e:
        logging.error("Erro ao listar %s: %s", remote_dir, e)
    return arquivos


def ensure_remote_dirs(sftp: pysftp.Connection, remote_dir: str) -> None:
    """Garante que o diretório remoto exista (criando partes intermediárias)."""
    remote_dir = remote_dir.replace("//", "/")
    if sftp.exists(remote_dir):
        return

    parts = remote_dir.strip("/").split("/")
    path = ""
    for part in parts:
        path += "/" + part
        if not sftp.exists(path):
            sftp.mkdir(path)


def load_job_config() -> cliente1JobConfig:
    base_dir = Path(__file__).resolve().parent
    load_env_file(base_dir / ".env")

    dt_exec = datetime.now().strftime("%Y%m%d_%H%M")
    log_filename = f"log_execucao_cliente1_{dt_exec}.log"

    temp_dir = base_dir / "CARGAS" / "cliente1"
    log_path = (base_dir / "logs" / "cliente1" / log_filename)

    origem = SFTPConfig(
        host=getenv_required("CLIENTE1_SFTP_HOST"),
        port=getenv_int("CLIENTE1_SFTP_PORT", 2222),
        username=getenv_required("CLIENTE1_SFTP_USER"),
        password=getenv_required("CLIENTE1_SFTP_PASS"),
        remote_dir=getenv_required("CLIENTE1_REMOTE_DIR"),
    )

    destino = SFTPConfig(
        host=getenv_required("PREAMBULO_SFTP_HOST"),
        port=getenv_int("PREAMBULO_SFTP_PORT", 22),
        username=getenv_required("PREAMBULO_SFTP_USER"),
        password=getenv_required("PREAMBULO_SFTP_PASS"),
        remote_dir="/",  # não usado diretamente
    )

    destino_uploads_dir = getenv_required("PREAMBULO_REMOTE_UPLOADS_DIR")
    destino_processados_dir = getenv_required("PREAMBULO_REMOTE_PROCESSADOS_DIR")
    destino_log_dir = getenv_required("PREAMBULO_REMOTE_LOG_DIR")

    disable_hostkey_check = getenv_bool("SFTP_DISABLE_HOSTKEY_CHECK", default=False)
    known_hosts_path = os.getenv("SFTP_KNOWN_HOSTS", "").strip() or None

    return cliente1JobConfig(
        origem=origem,
        destino=destino,
        destino_uploads_dir=destino_uploads_dir,
        destino_processados_dir=destino_processados_dir,
        destino_log_dir=destino_log_dir,
        base_dir=base_dir,
        temp_dir=temp_dir,
        log_path=log_path,
        log_filename=log_filename,
        disable_hostkey_check=disable_hostkey_check,
        known_hosts_path=known_hosts_path,
    )


def main() -> None:
    cfg = load_job_config()
    configurar_logs(cfg.log_path)

    status = StatusContador()
    cnopts = build_cnopts(cfg.disable_hostkey_check, cfg.known_hosts_path)
    data_hoje = datetime.today().date()

    # 1) Lista destino (uploads + processados)
    logging.info("Conectando ao SFTP de destino para listar arquivos (uploads + processados)...")
    with pysftp.Connection(
        cfg.destino.host,
        username=cfg.destino.username,
        password=cfg.destino.password,
        port=cfg.destino.port,
        cnopts=cnopts,
    ) as sftp_dest:
        arquivos_dest_uploads = listar_arquivos_sftp_recursivo(sftp_dest, cfg.destino_uploads_dir, filtro_data=data_hoje)
        arquivos_dest_proc = listar_arquivos_sftp_recursivo(sftp_dest, cfg.destino_processados_dir, filtro_data=data_hoje)

        arquivos_dest_rel: Dict[str, Dict[str, Any]] = {}
        for caminho, dados in {**arquivos_dest_uploads, **arquivos_dest_proc}.items():
            if "/uploads/" in caminho:
                rel = caminho.split("/uploads/", 1)[1]
            else:
                rel = caminho.replace(cfg.destino_uploads_dir.rstrip("/") + "/", "")
                rel = rel.replace(cfg.destino_processados_dir.rstrip("/") + "/", "")
            rel = rel.replace("\\", "/").lstrip("/")
            arquivos_dest_rel[rel] = dados

    # 2) Lista origem (apenas arquivos de hoje)
    logging.info("Conectando ao SFTP de origem para buscar arquivos do dia...")
    with pysftp.Connection(
        cfg.origem.host,
        username=cfg.origem.username,
        password=cfg.origem.password,
        port=cfg.origem.port,
        cnopts=cnopts,
    ) as sftp_orig:
        arquivos_origem = listar_arquivos_sftp_recursivo(sftp_orig, cfg.origem.remote_dir, filtro_data=data_hoje)

        for caminho_remoto, dados in arquivos_origem.items():
            rel_path = os.path.relpath(caminho_remoto, cfg.origem.remote_dir).replace("\\", "/")
            destino_path = f"{cfg.destino_uploads_dir.rstrip('/')}/{rel_path}".replace("//", "/")

            # Decide ação
            if rel_path in arquivos_dest_rel:
                tam_dest = arquivos_dest_rel[rel_path]["tamanho"]
                if tam_dest == dados["tamanho"]:
                    logging.info("Igual (em uploads ou processados): %s – pulando.", rel_path)
                    status.iguais += 1
                    continue
                else:
                    logging.info("Tamanho diferente: %s – removendo destino (se existir em uploads) e reenviando.", rel_path)
                    try:
                        with pysftp.Connection(
                            cfg.destino.host,
                            username=cfg.destino.username,
                            password=cfg.destino.password,
                            port=cfg.destino.port,
                            cnopts=cnopts,
                        ) as sftp_rm:
                            if sftp_rm.exists(destino_path):
                                sftp_rm.remove(destino_path)
                    except Exception as e:
                        logging.error("Erro ao remover %s: %s", destino_path, e)
                        status.erros_remocao += 1
                    status.reenviados += 1
            else:
                status.novos += 1

            # 3) Download local temporário
            try:
                local_temp = cfg.temp_dir / rel_path
                local_temp.parent.mkdir(parents=True, exist_ok=True)
                sftp_orig.get(caminho_remoto, str(local_temp))
            except Exception as e:
                logging.error("Erro ao baixar %s: %s", caminho_remoto, e)
                status.erros_download += 1
                continue

            # 4) Upload para destino
            try:
                with pysftp.Connection(
                    cfg.destino.host,
                    username=cfg.destino.username,
                    password=cfg.destino.password,
                    port=cfg.destino.port,
                    cnopts=cnopts,
                ) as sftp_envio:
                    pasta_dest = os.path.dirname(destino_path)
                    ensure_remote_dirs(sftp_envio, pasta_dest)
                    sftp_envio.put(str(local_temp), destino_path)
                    logging.info("Enviado: %s", destino_path)
            except Exception as e:
                logging.error("Erro ao enviar %s: %s", rel_path, e)
                status.erros_upload += 1

    status.log_resumo()

    # 5) Upload do log para destino
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
            logging.info("Log enviado para o SFTP de destino em: %s", cfg.destino_log_dir)
    except Exception as e:
        logging.error("Erro ao enviar log: %s", e)


if __name__ == "__main__":
    main()
