"""Подготовка и объединение источников для этапа 2.3."""
import argparse
import csv
import gzip
import json
import lzma
import math
import os
import re
import statistics
import ssl
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"
BANK_FILE = RAW / "laramee26openBankTransactionData.xlsx"
BINANCE_DIR = RAW / "binance"
NEWS_DIR = RAW / "news"
SEC_DIR = RAW / "sec"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
SEC_CIK = "0000019617"
SEC_CONTACT = os.getenv("SEC_CONTACT_INFO") or os.getenv("SEC_USER_AGENT", "")

for folder in (OUT, BINANCE_DIR, NEWS_DIR, SEC_DIR):
    folder.mkdir(parents=True, exist_ok=True)


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def save_csv(path, rows):
    fields = list(dict.fromkeys(key for row in rows for key in row))
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def get_url(url, headers=None, timeout=60):
    request_headers = {"User-Agent": "Course data project", "Accept-Encoding": "gzip"}
    request_headers.update(headers or {})
    context = ssl.create_default_context()
    try:
        import certifi
        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    request = urllib.request.Request(url, headers=request_headers)
    with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
        data = response.read()
        if response.headers.get("Content-Encoding", "").lower() == "gzip":
            data = gzip.decompress(data)
    return data


def parse_date(value):
    value = str(value or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    try:
        serial = float(value)
        return (datetime(1899, 12, 30) + timedelta(days=serial)).date() if 1 <= serial < 100000 else None
    except ValueError:
        return None


def parse_number(value):
    value = str(value or "").replace(",", "").replace("£", "").strip()
    try:
        return float(value) if value else None
    except ValueError:
        return None


def read_xlsx(path):
    """Читает значения ячеек XLSX без стороннего модуля."""
    import zipfile
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as book:
        shared = []
        if "xl/sharedStrings.xml" in book.namelist():
            root = ET.fromstring(book.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in item.findall(".//m:t", ns)) for item in root.findall("m:si", ns)]
        sheet_name = next(name for name in book.namelist() if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name))
        sheet = ET.fromstring(book.read(sheet_name))
    rows = []
    for row in sheet.findall(".//m:sheetData/m:row", ns):
        values = {}
        for cell in row.findall("m:c", ns):
            col = re.match(r"[A-Z]+", cell.attrib.get("r", "")).group()
            node = cell.find("m:v", ns)
            value = node.text if node is not None else ""
            if cell.attrib.get("t") == "s" and value:
                value = shared[int(value)]
            elif cell.attrib.get("t") == "inlineStr":
                value = "".join(t.text or "" for t in cell.findall(".//m:t", ns))
            values[col] = value or ""
        if values:
            rows.append(values)
    def col_number(name):
        result = 0
        for char in name:
            result = result * 26 + ord(char) - 64
        return result - 1
    table = []
    for row in rows:
        last = max(map(col_number, row))
        table.append([row.get(col_name(i + 1), "") for i in range(last + 1)])
    return table


def col_name(number):
    name = ""
    while number:
        number, rem = divmod(number - 1, 26)
        name = chr(65 + rem) + name
    return name


def load_bank():
    table = read_xlsx(BANK_FILE)
    headers = [str(value).strip() for value in table[0]]
    required = ["Transaction Number", "Transaction Date", "Transaction Type", "Transaction Description",
                "Debit Amount", "Credit Amount", "Balance", "Category", "Location City", "Location Country"]
    if any(field not in headers for field in required):
        raise ValueError(f"Банковская таблица имеет другую схему: {headers}")
    result, seen, invalid_dates, duplicates = [], set(), 0, 0
    for values in table[1:]:
        source = dict(zip(headers, values))
        day = parse_date(source["Transaction Date"])
        if not day:
            invalid_dates += 1
            continue
        key = str(source["Transaction Number"]).strip()
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        debit, credit = parse_number(source["Debit Amount"]), parse_number(source["Credit Amount"])
        amount = credit if credit is not None else -debit if debit is not None else ""
        result.append({
            "transaction_number": key, "date": day.isoformat(),
            "transaction_type": source["Transaction Type"].strip(),
            "description": source["Transaction Description"].strip(),
            "debit_amount": debit if debit is not None else "",
            "credit_amount": credit if credit is not None else "",
            "balance": parse_number(source["Balance"]) if parse_number(source["Balance"]) is not None else "",
            "category": source["Category"].strip(), "city": source["Location City"].strip(),
            "country": source["Location Country"].strip(), "amount_signed": amount,
            "amount_abs": abs(amount) if amount != "" else "",
            "flow_direction": "credit" if credit is not None else "debit" if debit is not None else "missing",
        })
    result.sort(key=lambda row: (row["date"], row["transaction_number"]))
    audit = {"rows_after": len(result), "invalid_dates_removed": invalid_dates,
             "duplicate_ids_removed": duplicates, "input_file": str(BANK_FILE.relative_to(ROOT))}
    return result, audit


def download_binance(start, end, refresh=False):
    start_ms = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000) - 1
    for symbol in SYMBOLS:
        path = BINANCE_DIR / f"{symbol.lower()}_1d.json"
        if path.exists() and not refresh:
            continue
        query = urllib.parse.urlencode({"symbol": symbol, "interval": "1d", "startTime": start_ms, "endTime": end_ms, "limit": 1000})
        payload = json.loads(get_url("https://api.binance.com/api/v3/klines?" + query))
        save_json(path, {"symbol": symbol, "rows": payload})


def download_news(refresh=False):
    base = "https://raw.githubusercontent.com/FelixDrinkall/financial-news-dataset/main/data"
    for year in (2021, 2022):
        path = NEWS_DIR / f"{year}_processed.json.xz"
        if not path.exists() or refresh:
            path.write_bytes(get_url(f"{base}/{year}_processed.json.xz", timeout=120))


def download_sec(refresh=False):
    if not SEC_CONTACT.strip():
        print("SEC пропущен: задайте переменную с названием проекта и контактным адресом.")
        return
    headers = {"User-Agent": SEC_CONTACT}
    url = f"https://data.sec.gov/submissions/CIK{SEC_CIK}.json"
    submissions = json.loads(get_url(url, headers))
    recent = submissions.get("filings", {}).get("recent", {})
    for i, form in enumerate(recent.get("form", [])):
        filed = recent.get("filingDate", [""])[i]
        if form not in ("10-K", "10-Q") or not filed.startswith(("2021", "2022")):
            continue
        accession = recent["accessionNumber"][i]
        report_date = recent.get("reportDate", [""])[i]
        acc = accession.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(SEC_CIK)}/{acc}/"
        index = json.loads(get_url(base + "index.json", headers))
        xmls = [item["name"] for item in index["directory"]["item"] if item.get("name", "").endswith(".xml") and item.get("type") == "EX-101.INS"]
        if xmls:
            path = SEC_DIR / f"{accession}.xml"
            if not path.exists() or refresh:
                path.write_bytes(get_url(base + xmls[0], headers))
                save_json(path.with_suffix(".meta.json"), {"form": form, "filed_date": filed,
                                                              "report_date": report_date, "accession": accession})
        time.sleep(0.12)


def load_binance():
    rows = []
    for path in BINANCE_DIR.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        symbol = payload.get("symbol", path.stem.split("_")[0].upper())
        for candle in payload.get("rows", []):
            rows.append({"date": datetime.fromtimestamp(int(candle[0]) / 1000, timezone.utc).date().isoformat(),
                         "symbol": symbol, "exchange": "Binance", "market_type": "Spot",
                         "base_asset": symbol[:-4], "quote_asset": symbol[-4:], "interval": "1d",
                         "open": float(candle[1]), "high": float(candle[2]), "low": float(candle[3]),
                         "close": float(candle[4]), "volume": float(candle[5]),
                         "quote_volume": float(candle[7]), "trade_count": int(candle[8])})
    return rows


def is_current(path, sources):
    sources = [source for source in sources if source.exists()]
    return bool(path.exists() and sources and path.stat().st_mtime_ns >= max(source.stat().st_mtime_ns for source in sources))


def load_news(rebuild=False):
    cache = OUT / "news_annotated.csv.gz"
    archives = list(NEWS_DIR.glob("*.json.xz"))
    if not rebuild and archives and is_current(cache, archives):
        numeric = {"sentiment_negative", "sentiment_neutral", "sentiment_positive", "word_count", "character_count"}
        csv.field_size_limit(10_000_000)
        with gzip.open(cache, "rt", encoding="utf-8-sig", newline="") as file:
            rows = list(csv.DictReader(file))
        for row in rows:
            for field in numeric:
                row[field] = float(row[field]) if row[field] else ""
        return rows
    rows = []
    for path in NEWS_DIR.glob("*.json.xz"):
        with lzma.open(path, "rt", encoding="utf-8") as file:
            for index, article in enumerate(read_json_array(file)):
                date_value = str(article.get("date_publish", ""))[:10]
                if not parse_date(date_value):
                    continue
                text = " ".join(str(article.get(key) or "") for key in ("title", "description", "maintext"))
                sent = article.get("sentiment") or {}
                scores = {name: float(sent.get(name, 0) or 0) for name in ("negative", "neutral", "positive")}
                topic = "banking" if re.search(r"bank|credit|loan|deposit", text, re.I) else "market" if re.search(r"stock|market|share|exchange", text, re.I) else "other"
                rows.append({"article_id": article.get("url") or f"{path.stem}_{index}", "date": date_value,
                             "source_domain": article.get("source_domain", ""), "language": article.get("language", ""),
                             "mentioned_companies": ";".join(map(str, article.get("mentioned_companies", []))),
                             "industries": ";".join(map(str, article.get("industries", []))),
                             "sentiment_negative": scores["negative"], "sentiment_neutral": scores["neutral"],
                             "sentiment_positive": scores["positive"], "sentiment_label": max(scores, key=scores.get),
                             "topic_annotation": topic, "word_count": len(text.split()),
                             "character_count": len(text), "text": text.strip()})
    return rows


def read_json_array(file):
    """Читает элементы большого JSON-массива по одному."""
    decoder = json.JSONDecoder()
    buffer = ""
    started = False
    while chunk := file.read(1024 * 1024):
        buffer += chunk
        if not started:
            start = buffer.find("[")
            if start < 0:
                continue
            buffer = buffer[start + 1:]
            started = True
        while True:
            buffer = buffer.lstrip(" \r\n\t,")
            if buffer.startswith("]"):
                return
            try:
                item, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                break
            yield item
            buffer = buffer[end:]


def load_sec_facts():
    facts = []
    for path in SEC_DIR.glob("*.xml"):
        root = ET.parse(path).getroot()
        contexts = {}
        units = {}
        for node in root.iter():
            if node.tag.endswith("context"):
                context = {}
                for item in node.iter():
                    tag = item.tag.rsplit("}", 1)[-1]
                    if tag in ("startDate", "endDate", "instant"):
                        context[tag] = item.text or ""
                contexts[node.attrib.get("id")] = context
            elif node.tag.endswith("unit"):
                units[node.attrib.get("id")] = "".join(part.text or "" for part in node.iter() if part is not node)
        metadata_path = path.with_suffix(".meta.json")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
        for node in root.iter():
            text = (node.text or "").strip()
            if not text or node.attrib.get("contextRef") is None:
                continue
            try:
                value = float(text.replace(",", ""))
            except ValueError:
                continue
            context = contexts.get(node.attrib["contextRef"], {})
            name = node.tag.rsplit("}", 1)[-1]
            facts.append({"issuer": "JPMorgan Chase", "cik": SEC_CIK, "form": metadata.get("form", ""),
                          "filed_date": metadata.get("filed_date", ""), "report_date": metadata.get("report_date", ""),
                          "accession": metadata.get("accession", path.stem), "concept": name,
                          "unit": units.get(node.attrib.get("unitRef"), ""), "value": value,
                          "period_start": context.get("startDate", ""),
                          "period_end": context.get("endDate", context.get("instant", "")),
                          "source_file": path.name})
    return facts


def sec_report_metrics(facts):
    selected = {}
    concepts = ("Assets", "Liabilities", "NetIncomeLoss")
    for fact in facts:
        if fact["concept"] not in concepts or fact["unit"] != "USD":
            continue
        key = (fact["accession"], fact["concept"])
        if key not in selected or fact["period_end"] > selected[key]["period_end"]:
            selected[key] = fact
    reports = {}
    for fact in selected.values():
        key = fact["accession"]
        row = reports.setdefault(key, {name: fact.get(name, "") for name in
                                       ("issuer", "cik", "form", "filed_date", "report_date", "accession")})
        row[fact["concept"]] = fact["value"]
    return list(reports.values())


def profile(name, rows, categorical, numeric, date_field="date"):
    fields = list(dict.fromkeys(key for row in rows for key in row))
    missing, stats, categories = [], [], []
    for field in fields:
        values = [row.get(field) for row in rows]
        blanks = sum(value in (None, "") for value in values)
        missing.append({"source": name, "field": field, "missing_count": blanks,
                        "missing_pct": round(blanks * 100 / len(rows), 3) if rows else 0})
    for field in numeric:
        values = [float(row[field]) for row in rows if row.get(field) not in (None, "")]
        if values:
            stats.append({"source": name, "field": field, "count": len(values), "min": min(values),
                          "max": max(values), "mean": statistics.fmean(values), "median": statistics.median(values)})
    for field in categorical:
        counts = Counter(str(row.get(field) or "<пусто>") for row in rows)
        categories.extend({"source": name, "field": field, "value": value, "count": count}
                          for value, count in counts.most_common(30))
    dates = [str(row.get(date_field, ""))[:10] for row in rows if row.get(date_field)]
    summary = {"source": name, "rows": len(rows), "columns": len(fields),
               "date_min": min(dates, default=""), "date_max": max(dates, default=""),
               "categorical_fields": categorical, "numeric_fields": numeric, "fields_actual": fields}
    return summary, missing, stats, categories


FIELD_MEANINGS = {
    "date": ("Дата наблюдения", "Временное сопоставление", "интервальная"),
    "transaction_type": ("Тип банковской операции", "Категория поведения", "номинальная"),
    "category": ("Категория операции", "Сегментация операций", "номинальная"),
    "debit_amount": ("Сумма списания", "Размер расхода", "отношений"),
    "credit_amount": ("Сумма зачисления", "Размер поступления", "отношений"),
    "balance": ("Остаток после операции", "Состояние счёта", "отношений"),
    "amount_signed": ("Зачисление плюс, списание минус", "Направленная сумма", "отношений; производная"),
    "amount_abs": ("Модуль суммы", "Размер операции без направления", "отношений; производная"),
    "flow_direction": ("Направление движения средств", "Разделение списаний и зачислений", "номинальная; производная"),
    "symbol": ("Торговая пара", "Рыночный фактор", "номинальная"),
    "open": ("Цена открытия свечи", "Уровень рынка", "отношений"),
    "high": ("Максимум свечи", "Размах рынка", "отношений"),
    "low": ("Минимум свечи", "Размах рынка", "отношений"),
    "close": ("Цена закрытия свечи", "Уровень рынка", "отношений"),
    "volume": ("Объём торгов", "Интенсивность рынка", "отношений"),
    "trade_count": ("Число сделок", "Интенсивность рынка", "счётная"),
    "concept": ("Понятие финансового отчёта XBRL", "Контекст факта", "номинальная"),
    "unit": ("Единица факта XBRL", "Проверка сопоставимости", "номинальная"),
    "value": ("Числовое значение факта XBRL", "Финансовый фактор", "отношений"),
    "language": ("Язык статьи", "Сегментация текста", "номинальная"),
    "source_domain": ("Домен издателя", "Происхождение новости", "номинальная"),
    "sentiment_positive": ("Оценка положительной тональности", "Текстовый фактор", "отношений"),
    "sentiment_negative": ("Оценка отрицательной тональности", "Текстовый фактор", "отношений"),
    "sentiment_neutral": ("Оценка нейтральной тональности", "Текстовый фактор", "отношений"),
    "topic_annotation": ("Тематическая метка по ключевым словам", "Предварительная разметка текста", "номинальная; производная"),
    "word_count": ("Число слов статьи", "Длина документа", "счётная; производная"),
    "text": ("Текст заголовка, описания и статьи", "Корпус для TF-IDF", "текстовая"),
}


def write_field_dictionary(source_rows, missing_rows):
    missing = {(row["source"], row["field"]): row["missing_pct"] for row in missing_rows}
    result = []
    for source, rows in source_rows.items():
        for field in list(dict.fromkeys(key for row in rows for key in row)):
            meaning, role, scale = FIELD_MEANINGS.get(field, ("Поле источника", "Контекст или сегментация", "номинальная"))
            values = [row.get(field) for row in rows if row.get(field) not in (None, "")]
            dtype = "числовой" if values and isinstance(values[0], (int, float)) else "текстовый/категориальный"
            result.append({"source": source, "field": field, "physical_meaning": meaning,
                           "analytical_role": role, "scale": scale, "dtype": dtype,
                           "missing_pct": missing.get((source, field), 0)})
    save_csv(OUT / "field_dictionary.csv", result)


def month(value):
    return str(value)[:7]


def aggregate(rows, key, metrics, prefix):
    groups = defaultdict(list)
    for row in rows:
        groups[month(row[key])].append(row)
    result = []
    for period, items in sorted(groups.items()):
        entry = {"month": period, prefix + "_count": len(items)}
        for output, field, operation in metrics:
            values = [float(item[field]) for item in items if item.get(field) not in (None, "")]
            entry[output] = operation(values) if values else ""
        result.append(entry)
    return result


def aggregate_crypto(rows):
    result = {}
    for symbol in SYMBOLS:
        selected = [row for row in rows if row["symbol"] == symbol]
        if not selected:
            continue
        prefix = "crypto_" + symbol.lower()
        for item in aggregate(selected, "date", [("close_mean", "close", statistics.fmean),
                                                   ("volume_sum", "volume", sum)], prefix):
            result.setdefault(item["month"], {"month": item["month"]}).update(item)
    return list(result.values())


def vectorize_news(rows):
    documents = [row for row in rows if row.get("language", "").lower() == "en"]
    stop = set("the and for that with this from are was were has have had not you your they their about into than then".split())
    docs = []
    df = Counter()
    for row in documents:
        words = [word for word in re.findall(r"[a-z]{3,}", row["text"].lower()) if word not in stop]
        counts = Counter(words)
        docs.append((row["article_id"], counts))
        df.update(counts.keys())
    terms = [term for term, count in df.most_common() if count >= 3][:500]
    term_set = set(terms)
    sparse_path = OUT / "news_tfidf_sparse.csv.gz"
    vocab = []
    for index, term in enumerate(terms):
        vocab.append({"term_index": index, "term": term, "document_frequency": df[term]})
    nonzero = 0
    with gzip.open(sparse_path, "wt", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["article_id", "term", "tfidf"])
        writer.writeheader()
        for article_id, counts in docs:
            weights = {term: count * (math.log((1 + len(docs)) / (1 + df[term])) + 1)
                       for term, count in counts.items() if term in term_set}
            norm = math.sqrt(sum(value * value for value in weights.values()))
            if norm:
                rows = [{"article_id": article_id, "term": term, "tfidf": value / norm}
                        for term, value in weights.items()]
                writer.writerows(rows)
                nonzero += len(rows)
    save_csv(OUT / "news_tfidf_vocabulary.csv", vocab)
    return {"status": "complete", "documents": len(docs), "features": len(terms), "nonzero_weights": nonzero}


def main():
    parser = argparse.ArgumentParser(description="Подготовка данных по пункту 2.3")
    parser.add_argument("--download", action="store_true", help="загрузить внешние источники")
    parser.add_argument("--refresh", action="store_true", help="обновить загруженные источники")
    parser.add_argument("--rebuild", action="store_true", help="пересчитать обработанные таблицы и TF-IDF")
    args = parser.parse_args()
    bank, bank_audit = load_bank()
    start = max(date.fromisoformat(bank[0]["date"]), date(2021, 1, 1))
    end = min(date.fromisoformat(bank[-1]["date"]), date(2022, 12, 31))
    if args.download or args.refresh:
        for name, action in (("Binance", lambda: download_binance(start, end, args.refresh)),
                             ("Новости", lambda: download_news(args.refresh)), ("SEC", lambda: download_sec(args.refresh))):
            try:
                action()
            except Exception as error:
                print(f"{name}: загрузка не удалась: {error}")

    crypto = load_binance()
    news = load_news(rebuild=args.rebuild)
    sec = load_sec_facts()
    sec_reports = sec_report_metrics(sec)
    sources = {"bank_transactions": bank, "binance_spot": crypto, "financial_news": news, "sec_xbrl": sec}
    configs = {
        "bank_transactions": (["transaction_type", "category", "city", "country"], ["debit_amount", "credit_amount", "balance", "amount_signed", "amount_abs"]),
        "binance_spot": (["exchange", "market_type", "symbol", "base_asset", "quote_asset"], ["open", "high", "low", "close", "volume", "quote_volume", "trade_count"]),
        "financial_news": (["source_domain", "language", "mentioned_companies", "industries", "topic_annotation", "sentiment_label"], ["sentiment_negative", "sentiment_neutral", "sentiment_positive", "word_count", "character_count"]),
        "sec_xbrl": (["issuer", "cik", "concept", "unit"], ["value"]),
    }
    profiles, missing, stats, categories = [], [], [], []
    for name, rows in sources.items():
        cats, nums = configs[name]
        if rows:
            p, m, n, c = profile(name, rows, cats, nums, "filed_date" if name == "sec_xbrl" else "date")
            profiles.append(p); missing += m; stats += n; categories += c
            output_name = "news_annotated.csv.gz" if name == "financial_news" else f"{name}.csv"
            target = OUT / output_name
            inputs = list(NEWS_DIR.glob("*.json.xz")) if name == "financial_news" else [BANK_FILE] if name == "bank_transactions" else []
            if args.rebuild or not is_current(target, inputs):
                save_csv(target, rows)
        else:
            profiles.append({"source": name, "status": "not_downloaded", "rows": 0,
                             "categorical_fields": cats, "numeric_fields": nums})
    if sec_reports:
        p, m, n, c = profile("sec_report_metrics", sec_reports, ["issuer", "cik", "form"],
                             ["Assets", "Liabilities", "NetIncomeLoss"], "filed_date")
        profiles.append(p); missing += m; stats += n; categories += c
        save_csv(OUT / "sec_report_metrics.csv", sec_reports)
    save_json(OUT / "source_profiles.json", profiles)
    save_csv(OUT / "missingness_and_schema.csv", missing)
    save_csv(OUT / "numeric_descriptives.csv", stats)
    save_csv(OUT / "categorical_levels.csv", categories)
    bank_months = aggregate(bank, "date", [("amount_abs_sum", "amount_abs", sum),
                                             ("amount_abs_mean", "amount_abs", statistics.fmean)], "bank")
    save_csv(OUT / "bank_monthly.csv", bank_months)
    crypto_months = aggregate_crypto(crypto) if crypto else []
    news_months = aggregate(news, "date", [("word_count_mean", "word_count", statistics.fmean)], "news") if news else []
    if crypto_months:
        save_csv(OUT / "crypto_monthly.csv", crypto_months)
    if news_months:
        save_csv(OUT / "news_monthly.csv", news_months)
    crypto_by_month = {row["month"]: row for row in crypto_months}
    news_by_month = {row["month"]: row for row in news_months}
    integrated = [{**row, **crypto_by_month[row["month"]], **news_by_month[row["month"]]}
                  for row in bank_months if row["month"] in crypto_by_month and row["month"] in news_by_month]
    sec_reports.sort(key=lambda row: row.get("filed_date", ""))
    for row in integrated:
        month_end = (datetime.strptime(row["month"] + "-01", "%Y-%m-%d").date() + timedelta(days=35)).replace(day=1) - timedelta(days=1)
        eligible = [report for report in sec_reports if report.get("filed_date") and report["filed_date"] <= month_end.isoformat()]
        latest = eligible[-1] if eligible else {}
        for field in ("Assets", "Liabilities", "NetIncomeLoss"):
            row["sec_" + field.lower()] = latest.get(field, "")
    save_csv(OUT / "integrated_monthly.csv", integrated)
    summary_path = OUT / "text_vectorization_summary.json"
    sparse_path = OUT / "news_tfidf_sparse.csv.gz"
    vocabulary_path = OUT / "news_tfidf_vocabulary.csv"
    news_sources = list(NEWS_DIR.glob("*.json.xz"))
    english_count = sum(row.get("language", "").lower() == "en" for row in news)
    if (news and not args.rebuild and is_current(sparse_path, news_sources)
            and is_current(vocabulary_path, news_sources) and is_current(summary_path, news_sources)):
        tfidf = json.loads(summary_path.read_text(encoding="utf-8"))
        if tfidf.get("status") != "complete" or tfidf.get("documents") != english_count:
            tfidf = vectorize_news(news)
            save_json(summary_path, tfidf)
    else:
        tfidf = vectorize_news(news) if news else {"status": "not run", "reason": "новости не загружены"}
        save_json(summary_path, tfidf)
    save_json(OUT / "cleaning_audit.json", [bank_audit])
    write_field_dictionary(sources, missing)
    samples = [f"## {name}\n\n" + (json.dumps(rows[:3], ensure_ascii=False, indent=2) if rows else "Источник не загружен.")
               for name, rows in sources.items()]
    (OUT / "sample_rows.md").write_text("# Примеры фактических наблюдений\n\n" + "\n\n".join(samples), encoding="utf-8")
    save_json(OUT / "integration_audit.json", {"bank_months": len(bank_months), "crypto_months": len(crypto_months),
              "news_months": len(news_months), "common_months": len(integrated),
              "merge_key": "calendar month", "sec_facts_as_of": "в исходном XML сохраняется дата подачи; не переносить значения в прошлое"})
    save_json(OUT / "run_manifest.json", {
        "run_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "sources": {name: {"rows": len(rows), "status": "loaded" if rows else "not_downloaded"}
                    for name, rows in sources.items()},
        "integrated_months": len(integrated),
        "outputs": ["bank_transactions.csv", "binance_spot.csv", "news_annotated.csv.gz",
                    "sec_xbrl.csv", "bank_monthly.csv", "crypto_monthly.csv", "news_monthly.csv",
                    "integrated_monthly.csv", "source_profiles.json", "field_dictionary.csv",
                    "sample_rows.md", "text_vectorization_summary.json"]
    })
    print(f"Банковские операции: {len(bank)} ({bank[0]['date']} — {bank[-1]['date']})")
    print(f"Binance: {len(crypto)} свечей; SEC: {len(sec)} фактов; новости: {len(news)}")
    print(f"Общих месяцев: {len(integrated)}; TF-IDF: {tfidf}")
    print(f"Результаты: {OUT}")


if __name__ == "__main__":
    main()
