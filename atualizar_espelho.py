# atualizar_espelho_resiliente.py
# -*- coding: utf-8 -*-

import re
import random
import time
import os
import json
import base64
from datetime import datetime, date
from typing import List, Any, Dict, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# CONFIG
# =========================
CAMINHO_CRED = "credenciais.json"

ID_ORIGEM   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "BD_Carteira"

ID_DESTINO  = "1sTPEI6kLfqTFVRelm96reKp_SPtgRWqH5VGabAd72WE"
ABA_DESTINO = "Base de dados (Espelho)"

# Mapeamento: (coluna_origem, coluna_destino, tipo)
MAPPINGS = [
    ("AC", "B", "texto"),
    ("AA", "C", "texto"),
    ("Z" , "D", "texto"),
    ("AK", "E", "texto"),
    ("A" , "F", "texto"),
    ("B" , "G", "texto"),
    ("H" , "H", "texto"),
    ("AJ", "I", "texto"),
    ("I" , "J", "texto"),
    ("Q" , "K", "data"),
    ("R" , "L", "data"),
    ("M" , "M", "data"),
    ("AL", "N", "data"),
    ("AM", "O", "data"),
    ("N" , "P", "data"),
    ("J" , "Q", "valor"),
    ("K" , "R", "valor"),
    ("Y" , "S", "valor"),
    ("X" , "T", "valor"),
    ("AB", "U", "valor"),
    ("AF", "V", "valor"),
    ("AE", "W", "valor"),
    ("AN", "X", "texto"),
]

DEST_START_COL = "B"
DEST_END_COL   = "X"
DEST_HEADER_ROW = 2      # Cabeçalho na linha 2 (destino)
DEST_DATA_START_ROW = 3  # Dados a partir da linha 3 (destino)
BATCH_ROWS = 3000
MAX_ROWS_SCAN = 120_000
MAX_RUN_TRIES = 3  # reiniciar o fluxo completo se tudo falhar

# =========================
# Retry helpers
# =========================
def is_retryable_api_error(e: APIError) -> bool:
    """Detecta 429/5xx de forma robusta no APIError do gspread."""
    s = str(e)
    # Aceita padrões [503], 503, status: 503, etc.
    return any(code in s for code in ("429", "500", "502", "503"))

def with_retry(call, desc: str, max_tries: int = 9, base: float = 0.8, cap: float = 12.0):
    """
    Executa `call()` com retry exponencial + jitter para 429/5xx.
    """
    attempt = 1
    while True:
        try:
            return call()
        except APIError as e:
            if not is_retryable_api_error(e) or attempt >= max_tries:
                print(f"❌ {desc}: falhou (tentativa {attempt}/{max_tries}) → {e}")
                raise
            delay = min(cap, base * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
            print(f"⚠️ {desc}: erro transitório ({e}). Retry em {delay:.1f}s (tentativa {attempt}/{max_tries})…")
            time.sleep(delay)
            attempt += 1
        except Exception as e:
            # Retries leves para erros inesperados de rede
            if attempt >= min(3, max_tries):
                print(f"❌ {desc}: falhou (tentativa {attempt}) → {e}")
                raise
            delay = 1.0 + random.uniform(0, 0.5)
            print(f"⚠️ {desc}: erro inesperado ({e}). Retry leve em {delay:.1f}s…")
            time.sleep(delay)
            attempt += 1

# =========================
# Utils
# =========================
def col_letter_to_index(col: str) -> int:
    col = col.upper()
    out = 0
    for ch in col:
        out = out * 26 + (ord(ch) - ord('A') + 1)
    return out

def index_to_col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(r + 65) + s
    return s

def a1_range(start_col: str, start_row: int, end_col: str, end_row: int) -> str:
    return f"{start_col}{start_row}:{end_col}{end_row}"

def clean_currency(value: Any) -> Any:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    s = re.sub(r"[^0-9,\.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return ""

def to_gs_serial(dt: Any) -> Any:
    if dt is None or dt == "":
        return ""
    if isinstance(dt, (int, float)):
        return dt
    if isinstance(dt, (datetime, date)):
        py_date = dt.date() if isinstance(dt, datetime) else dt
    else:
        s = str(dt).strip()
        if not s:
            return ""
        py_date = None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
            try:
                py_date = datetime.strptime(s, fmt).date()
                break
            except ValueError:
                pass
        if py_date is None:
            return ""
    epoch = date(1899, 12, 30)
    return float((py_date - epoch).days)

def get_grid_size(wks) -> Tuple[int, int]:
    def _call():
        meta = wks.spreadsheet.fetch_sheet_metadata()
        for sh in meta["sheets"]:
            if sh["properties"]["title"] == wks.title:
                rows = sh["properties"]["gridProperties"]["rowCount"]
                cols = sh["properties"]["gridProperties"]["columnCount"]
                return rows, cols
        return 2000, 26
    return with_retry(_call, "fetch_sheet_metadata")

def auto_expand_rows(wks, needed_rows: int):
    rows, _ = get_grid_size(wks)
    if rows < needed_rows:
        with_retry(lambda: wks.add_rows(needed_rows - rows),
                   f"add_rows({needed_rows - rows})")

def clear_all(wks):
    rows, cols = get_grid_size(wks)
    last_col_letter = index_to_col_letter(cols)
    rng = a1_range("A", 1, last_col_letter, rows)
    return with_retry(lambda: wks.batch_clear([rng]), f"batch_clear {rng}")

def set_matrix(wks, start_col: str, start_row: int, end_col: str, end_row: int,
               values: List[List[Any]]):
    rng = a1_range(start_col, start_row, end_col, end_row)
    return with_retry(lambda: wks.update(rng, values, value_input_option="RAW"),
                      f"update {rng}")

def batch_get_cols(wks, col_ranges: List[str], unformatted=True, serial_dates=True) -> List[List[List[Any]]]:
    def _call():
        return wks.batch_get(
            col_ranges,
            value_render_option="UNFORMATTED_VALUE" if unformatted else "FORMATTED_VALUE",
            date_time_render_option="SERIAL_NUMBER" if serial_dates else "FORMATTED_STRING",
            major_dimension=None,
        )
    return with_retry(_call, f"batch_get {len(col_ranges)} ranges")

def calc_num_rows_from_columns(cols_dict: Dict[str, List[Any]]) -> int:
    """
    Determina a quantidade de linhas de dados observando TODAS as colunas mapeadas,
    removendo 'trailing blanks'. Retorna 0 se não houver dados.
    """
    max_len = 0
    for col, values in cols_dict.items():
        # tira vazios no final
        v = list(values)
        while v and (v[-1] == "" or str(v[-1]).strip() == ""):
            v.pop()
        if len(v) > max_len:
            max_len = len(v)
    return max_len

# =========================
# Main (com reinício do fluxo)
# =========================
def run_once():
    print("🔐 Autenticando…")
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]

    # Tenta usar credenciais do ambiente (GitHub Actions) via base64
    b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")

    if b64:
        try:
            raw = base64.b64decode(b64)
            info = json.loads(raw.decode("utf-8"))
            print("🔑 Usando credenciais do ambiente (GOOGLE_CREDENTIALS_B64).")
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as e:
            print(f"❌ Erro ao decodificar GOOGLE_CREDENTIALS_B64: {e}")
            raise
    else:
        # Caminho local: usa arquivo credenciais.json
        print(f"📁 GOOGLE_CREDENTIALS_B64 não encontrada. Usando arquivo {CAMINHO_CRED}.")
        creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=scopes)

    gc = gspread.authorize(creds)
    print("✅ Autenticado.")

    print("📂 Abrindo planilha ORIGEM…")
    wks_src = with_retry(lambda: gc.open_by_key(ID_ORIGEM).worksheet(ABA_ORIGEM),
                         f"open origem '{ABA_ORIGEM}'")

    print("📂 Abrindo planilha DESTINO…")
    wks_dst = with_retry(lambda: gc.open_by_key(ID_DESTINO).worksheet(ABA_DESTINO),
                         f"open destino '{ABA_DESTINO}'")

    # ——— Cabeçalho (linha 3 da origem) ———
    print("🧾 Lendo cabeçalho da ORIGEM (linha 3)…")
    headers: List[Any] = []
    for src_col, _, _tipo in MAPPINGS:
        vals = with_retry(lambda: wks_src.get(f"{src_col}3",
                                              value_render_option="UNFORMATTED_VALUE",
                                              date_time_render_option="SERIAL_NUMBER"),
                          f"get {src_col}3")
        headers.append(vals[0][0] if (vals and vals[0]) else "")

    # ——— Dados por batch_get (todas as colunas mapeadas) ———
    start_row = 4
    end_row_guess = start_row + MAX_ROWS_SCAN - 1
    ranges = [f"{src_col}{start_row}:{src_col}{end_row_guess}" for src_col, _, _ in MAPPINGS]
    print(f"📥 Lendo colunas mapeadas em batch ({len(ranges)} ranges)…")
    batch = batch_get_cols(wks_src, ranges, unformatted=True, serial_dates=True)

    # Normaliza para dict {coluna_origem: lista}
    data_cols: Dict[str, List[Any]] = {}
    for i, (src_col, _, _tipo) in enumerate(MAPPINGS):
        col_vals_raw = batch[i] if i < len(batch) else []
        # wks.batch_get retorna lista de linhas (cada linha é uma lista com 1 célula)
        # Converter p/ lista simples de tamanho MAX_ROWS_SCAN
        flat = [row[0] if row else "" for row in col_vals_raw]
        data_cols[src_col] = flat

    num_rows_data = calc_num_rows_from_columns(data_cols)
    print(f"🔎 Linhas de dados detectadas: {num_rows_data}")

    # Tratar tipos
    print("🧪 Tratando dados (datas/valores)…")
    treated_matrix: List[List[Any]] = []
    for r in range(num_rows_data):
        row_out = []
        for src_col, _, tipo in MAPPINGS:
            v = data_cols[src_col][r] if r < len(data_cols[src_col]) else ""
            if tipo == "valor":
                v = clean_currency(v)
            elif tipo == "data":
                if not isinstance(v, (int, float)):
                    v = to_gs_serial(v)
            row_out.append(v)
        treated_matrix.append(row_out)

    # ——— DESTINO: limpar e escrever ———
    print("🧹 Limpando TUDO na aba de destino…")
    clear_all(wks_dst)

    total_to_write = (1 if headers else 0) + num_rows_data
    end_row = max(DEST_DATA_START_ROW, DEST_HEADER_ROW) + total_to_write - 1
    print(f"🧱 Ajustando linhas até {end_row}…")
    auto_expand_rows(wks_dst, end_row)

    print("📝 Escrevendo cabeçalho na linha 2…")
    set_matrix(wks_dst, DEST_START_COL, DEST_HEADER_ROW, DEST_END_COL, DEST_HEADER_ROW, [headers])

    if treated_matrix:
        print(f"📤 Escrevendo {num_rows_data} linha(s) de dados a partir da linha 3…")
        start_row_dst = DEST_DATA_START_ROW
        i = 0
        while i < num_rows_data:
            chunk = treated_matrix[i:i+BATCH_ROWS]
            chunk_end = start_row_dst + len(chunk) - 1
            set_matrix(wks_dst, DEST_START_COL, start_row_dst, DEST_END_COL, chunk_end, chunk)
            i += len(chunk)
            start_row_dst = chunk_end + 1

    print("✅ Concluído com sucesso!")

def main():
    for attempt in range(1, MAX_RUN_TRIES + 1):
        try:
            run_once()
            return
        except APIError as e:
            if is_retryable_api_error(e) and attempt < MAX_RUN_TRIES:
                # Reseta o fluxo inteiro (nova sessão/handles ajuda quando o backend do Google está instável)
                delay = 5.0 * attempt + random.uniform(0, 1.5)
                print(f"⚠️ APIError final nesta passada ({e}). Reiniciando fluxo em {delay:.1f}s (tentativa {attempt}/{MAX_RUN_TRIES})…")
                time.sleep(delay)
                continue
            print("❌ Falhou mesmo após reinício do fluxo.")
            raise
        except Exception as e:
            if attempt < MAX_RUN_TRIES:
                delay = 3.0 + random.uniform(0, 1.0)
                print(f"⚠️ Erro inesperado nesta passada ({e}). Reiniciando fluxo em {delay:.1f}s (tentativa {attempt}/{MAX_RUN_TRIES})…")
                time.sleep(delay)
                continue
            print("❌ Falhou mesmo após reinício do fluxo (erro inesperado).")
            raise

if __name__ == "__main__":
    main()
