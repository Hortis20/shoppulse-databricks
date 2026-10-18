# Databricks notebook source
OUTPUT_DIR = "/Volumes/shoppulse/dev/raw/generated"

# COMMAND ----------

"""
Générateur de transactions ShopPulse (JSON Lines) pour le projet Databricks.

Produit 2 lots :
  - batch_01 : données initiales (~5 000 transactions sur 12 mois)
  - batch_02 : arrivée suivante, conçue pour tester le pipeline :
        * nouvelles transactions            -> INSERT du MERGE
        * transactions PENDING devenues COMPLETED (même transaction_id) -> UPDATE du MERGE
        * doublons stricts (renvois)        -> déduplication Silver
        * quelques dates dans un autre format -> cast en TimestampType

Utilisation :
  - En local :      python generate_transactions.py --output ./data
  - Dans Databricks (cellule de notebook) : définir OUTPUT_DIR sur le Volume,
    ex. "/Volumes/shoppulse/dev/raw/transactions", puis exécuter le fichier.

Aucune dépendance externe (bibliothèque standard uniquement).
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta

SEED = 42
N_USERS = 500
N_PRODUCTS = 200
BATCH1_SIZE = 5000
BATCH2_NEW = 1000
BATCH2_UPDATES = 150      # PENDING -> COMPLETED
DUPLICATE_RATE = 0.03     # part de doublons stricts dans chaque lot
BAD_DATE_RATE = 0.01      # part de dates au format "dd/MM/yyyy HH:mm:ss"
FILES_PER_BATCH = 4

START = datetime(2025, 10, 1)
END_BATCH1 = datetime(2026, 9, 1)
END_BATCH2 = datetime(2026, 10, 1)

STATUSES = ["COMPLETED", "PENDING", "CANCELLED"]
STATUS_WEIGHTS = [0.70, 0.20, 0.10]

rng = random.Random(SEED)

USERS = [f"U{i:04d}" for i in range(1, N_USERS + 1)]
# Quelques "gros clients" achètent beaucoup plus que les autres (Top 10 intéressant)
USER_WEIGHTS = [rng.paretovariate(1.5) for _ in USERS]
PRODUCTS = {f"P{i:03d}": round(rng.uniform(5, 400), 2) for i in range(1, N_PRODUCTS + 1)}

_tx_counter = 0


def next_tx_id() -> str:
    global _tx_counter
    _tx_counter += 1
    return f"TX{_tx_counter:08d}"


def random_date(start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=rng.uniform(0, delta))


def format_date(d: datetime) -> str:
    if rng.random() < BAD_DATE_RATE:
        return d.strftime("%d/%m/%Y %H:%M:%S")   # format "sale"
    return d.strftime("%Y-%m-%dT%H:%M:%S")      # ISO 8601


def make_transaction(start: datetime, end: datetime, status: str | None = None) -> dict:
    product_id = rng.choice(list(PRODUCTS))
    qty = rng.choices([1, 2, 3], weights=[0.75, 0.2, 0.05])[0]
    return {
        "transaction_id": next_tx_id(),
        "user_id": rng.choices(USERS, weights=USER_WEIGHTS)[0],
        "product_id": product_id,
        "amount": round(PRODUCTS[product_id] * qty, 2),
        "transaction_date": format_date(random_date(start, end)),
        "status": status or rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
    }


def add_duplicates(rows: list[dict]) -> list[dict]:
    dups = [dict(r) for r in rng.sample(rows, int(len(rows) * DUPLICATE_RATE))]
    out = rows + dups
    rng.shuffle(out)
    return out


def write_batch(rows: list[dict], output_dir: str, batch_name: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    chunk = -(-len(rows) // FILES_PER_BATCH)  # division arrondie au supérieur
    for i in range(FILES_PER_BATCH):
        part = rows[i * chunk:(i + 1) * chunk]
        path = os.path.join(output_dir, f"{batch_name}_part{i + 1:02d}.json")
        with open(path, "w", encoding="utf-8") as f:
            for r in part:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  {path} ({len(part)} lignes)")


def main(output_dir: str) -> None:
    # ---- Lot 1 : données initiales
    batch1 = [make_transaction(START, END_BATCH1) for _ in range(BATCH1_SIZE)]

    # ---- Lot 2 : nouveautés + mises à jour de statut + renvois
    batch2_new = [make_transaction(END_BATCH1, END_BATCH2) for _ in range(BATCH2_NEW)]

    pending = [r for r in batch1 if r["status"] == "PENDING"]
    batch2_updates = []
    for r in rng.sample(pending, min(BATCH2_UPDATES, len(pending))):
        upd = dict(r)
        upd["status"] = "COMPLETED"
        batch2_updates.append(upd)

    resent = [dict(r) for r in rng.sample(batch1, 50)]  # lignes du lot 1 renvoyées telles quelles

    batch1_final = add_duplicates(batch1)
    batch2_final = add_duplicates(batch2_new + batch2_updates + resent)

    print("Lot 1 :")
    write_batch(batch1_final, os.path.join(output_dir, "batch_01"), "transactions_batch_01")
    print("Lot 2 :")
    write_batch(batch2_final, os.path.join(output_dir, "batch_02"), "transactions_batch_02")

    print(
        f"\nRésumé : lot 1 = {len(batch1_final)} lignes, "
        f"lot 2 = {len(batch2_final)} lignes "
        f"({len(batch2_new)} nouvelles, {len(batch2_updates)} PENDING->COMPLETED, "
        f"{len(resent)} renvois, + doublons)"
    )


if __name__ == "__main__":
    try:
        OUTPUT_DIR  # défini dans une cellule Databricks
    except NameError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--output", default="./data")
        args, _ = parser.parse_known_args()
        OUTPUT_DIR = args.output
    main(OUTPUT_DIR)

# COMMAND ----------

src = "/Volumes/shoppulse/dev/raw/generated/batch_01"
dst = "/Volumes/shoppulse/dev/raw/landing"

dbutils.fs.mkdirs(dst)                       # 1. crée le dossier landing
for f in dbutils.fs.ls(src):                 # 2. pour chaque fichier du lot 1...
    dbutils.fs.cp(f.path, f"{dst}/{f.name}") #    ...on le copie dans landing

display(dbutils.fs.ls(dst))                  # 3. affiche le contenu de landing

# COMMAND ----------

# MAGIC %md
# MAGIC test du merge de la silver avec le lot 2

# COMMAND ----------

src = "/Volumes/shoppulse/dev/raw/generated/batch_02"
dst = "/Volumes/shoppulse/dev/raw/landing"

dbutils.fs.mkdirs(dst)                       # 1. crée le dossier landing
for f in dbutils.fs.ls(src):                 # 2. pour chaque fichier du lot 1...
    dbutils.fs.cp(f.path, f"{dst}/{f.name}") #    ...on le copie dans landing

display(dbutils.fs.ls(dst))                  # 3. affiche le contenu de landing

# COMMAND ----------

src = "/Volumes/shoppulse/dev/raw/generated/batch_02"
dst = "/Volumes/shoppulse/prod/raw/landing"

for f in dbutils.fs.ls(src):
    dbutils.fs.cp(f.path, f"{dst}/{f.name}")

display(dbutils.fs.ls(dst))

# COMMAND ----------

