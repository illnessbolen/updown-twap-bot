#!/usr/bin/env python3
"""
Одноразовый скрипт: получить CLOB API-ключ (apiKey/secret/passphrase) для PolyBolt.

Запускает человек вручную. Бот приватный ключ не видит: в .env попадают только
POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE и адрес (он не секретный).

Протокол (docs/API_NOTES.md, §1.1; https://docs.polymarket.com/getting-started/api.md):
  1. EIP-712 подпись ClobAuth приватным ключом (L1).
  2. POST https://clob.polymarket.com/auth/api-key (создать);
     если 400 - ключ уже есть, GET /auth/derive-api-key (получить существующий).
Так же делает официальный SDK polymarket-client 0.12.0 (_internal/l1_auth.py, actions/auth.py).

Примеры:
  # новый кошелёк: ключ сохраняется в файл с правами 600, потом получаем API-ключ
  python scripts/get_clob_api_key.py --new-wallet ~/.updown-bot/wallet.key

  # уже есть файл с приватным ключом
  python scripts/get_clob_api_key.py --key-file ~/.updown-bot/wallet.key

  # ввести ключ вручную (ввод не отображается)
  python scripts/get_clob_api_key.py

Зависимость: pip install "eth-account>=0.13,<1"
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import stat
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CLOB_URL = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon
CLOB_AUTH_MESSAGE = "This message attests that I control the given wallet"


def clob_auth_typed_data(address: str, timestamp: int, nonce: int = 0) -> dict:
    """Типизированные данные ClobAuth в точности как в документации и SDK."""
    return {
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": CHAIN_ID},
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "ClobAuth": [
                {"name": "address", "type": "address"},
                {"name": "timestamp", "type": "string"},
                {"name": "nonce", "type": "uint256"},
                {"name": "message", "type": "string"},
            ],
        },
        "primaryType": "ClobAuth",
        "message": {
            "address": address,
            "timestamp": str(timestamp),
            "nonce": nonce,
            "message": CLOB_AUTH_MESSAGE,
        },
    }


def l1_headers(account, timestamp: int, nonce: int = 0) -> dict[str, str]:
    signed = account.sign_typed_data(
        full_message=clob_auth_typed_data(account.address, timestamp, nonce))
    return {
        "POLY_ADDRESS": account.address,
        "POLY_SIGNATURE": "0x" + bytes(signed.signature).hex(),
        "POLY_TIMESTAMP": str(timestamp),
        "POLY_NONCE": str(nonce),
    }


def _request(method: str, path: str, headers: dict[str, str] | None = None) -> tuple[int, str]:
    # Cloudflare отвечает 403 "error code: 1010" на User-Agent по умолчанию "Python-urllib"
    hdrs = {"User-Agent": "updown-twap-bot/0.1", "Accept": "application/json", **(headers or {})}
    req = urllib.request.Request(CLOB_URL + path, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def check_clock_skew() -> None:
    """Подпись содержит время; при сильно сбитых часах сервер её отклонит."""
    status, body = _request("GET", "/time")
    if status != 200:
        return
    try:
        skew = int(time.time()) - int(float(body.strip()))
    except ValueError:
        return
    if abs(skew) > 10:
        print(f"ВНИМАНИЕ: часы расходятся с сервером на {skew} с. Синхронизируйте время.",
              file=sys.stderr)


def create_or_derive(account, nonce: int = 0) -> dict:
    status, body = _request("POST", "/auth/api-key", l1_headers(account, int(time.time()), nonce))
    if status == 400:  # ключ для этого адреса и nonce уже существует
        status, body = _request("GET", "/auth/derive-api-key",
                                l1_headers(account, int(time.time()), nonce))
    if status != 200:
        hint = ""
        if status in (401, 403):
            hint = (" (401: подпись или время не приняты; 403: доступ запрещён, "
                    "например гео-ограничение или прокси)")
        raise SystemExit(f"Ошибка CLOB: HTTP {status}{hint}: {body[:300]}")
    creds = json.loads(body)
    if not all(isinstance(creds.get(k), str) for k in ("apiKey", "secret", "passphrase")):
        raise SystemExit("Неожиданный формат ответа CLOB (нет apiKey/secret/passphrase).")
    return creds


def write_private_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)  # не перезаписываем
    with os.fdopen(fd, "w") as f:
        f.write(text)


def update_env_file(path: Path, values: dict[str, str], force: bool) -> None:
    lines = path.read_text().splitlines() if path.exists() else []
    present = {ln.split("=", 1)[0].strip() for ln in lines if "=" in ln and not ln.startswith("#")}
    clash = present & set(values)
    if clash and not force:
        raise SystemExit(f"В {path} уже есть {sorted(clash)}. Запустите с --force, чтобы заменить.")
    kept = [ln for ln in lines if ln.split("=", 1)[0].strip() not in values]
    kept += [f"{k}={v}" for k, v in values.items()]
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(kept) + "\n")
    os.replace(tmp, path)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def load_account(args):
    from eth_account import Account

    if args.new_wallet:
        path = Path(args.new_wallet).expanduser()
        if path.exists():
            raise SystemExit(f"{path} уже существует, не перезаписываю.")
        account = Account.create()
        write_private_file(path, "0x" + bytes(account.key).hex() + "\n")
        print(f"Новый кошелёк создан, приватный ключ сохранён в {path} (права 600).")
        print("Сделайте резервную копию этого файла. Без него ключ не восстановить.")
        return account
    if args.key_file:
        key = Path(args.key_file).expanduser().read_text().strip()
    else:
        key = getpass.getpass("Приватный ключ (ввод скрыт): ").strip()
    return Account.from_key(key)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    src = p.add_mutually_exclusive_group()
    src.add_argument("--new-wallet", metavar="PATH", help="создать новый кошелёк и сохранить ключ в PATH")
    src.add_argument("--key-file", metavar="PATH", help="файл с приватным ключом (hex)")
    p.add_argument("--env-file", default=".env", help="куда записать ключи API (по умолчанию .env)")
    p.add_argument("--force", action="store_true", help="заменить существующие POLY_API_* в env-файле")
    p.add_argument("--nonce", type=int, default=0, help="nonce набора ключей (по умолчанию 0)")
    args = p.parse_args()

    try:
        import eth_account  # noqa: F401
    except ImportError:
        raise SystemExit('Нужен eth-account: pip install "eth-account>=0.13,<1"')

    account = load_account(args)
    print(f"Адрес кошелька: {account.address}")
    check_clock_skew()
    creds = create_or_derive(account, args.nonce)

    env_path = Path(args.env_file)
    update_env_file(env_path, {
        "POLY_API_KEY": creds["apiKey"],
        "POLY_API_SECRET": creds["secret"],
        "POLY_API_PASSPHRASE": creds["passphrase"],
        "POLY_ADDRESS": account.address,
    }, args.force)
    print(f"Готово: ключи записаны в {env_path.resolve()} (права 600). "
          f"apiKey начинается с {creds['apiKey'][:8]}…")
    print("Секрет и passphrase на экран не выводятся. Не коммитьте .env.")


if __name__ == "__main__":
    main()
