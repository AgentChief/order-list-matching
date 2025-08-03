@echo off
REM ╔═══════════════════════════════════════════╗
REM ║  Order‑Match‑LM first‑run bootstrap       ║
REM ╚═══════════════════════════════════════════╝

SETLOCAL ENABLEDELAYEDEXPANSION

REM 1. Create virtual‑env
python -m venv .venv
CALL .venv\Scripts\activate

REM 2. Install requirements
pip install -U pip >NUL
pip install -r requirements.txt

REM 3. Initial folders
for %%D in (reports config src\core) do if not exist %%D mkdir %%D

REM 4. Smoke test (comment out until DB creds are real)
REM python src\reconcile.py --customer GREYSON --po 4755

echo(
echo ✅  Repo ready.  Edit config\config.yaml then run:
echo    .venv\Scripts\python  src\reconcile.py --customer GREYSON --po 4755
