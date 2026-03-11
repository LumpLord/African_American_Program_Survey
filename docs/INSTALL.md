# Installation

This project is a Python-based pipeline for building and analyzing a program-inventory dataset (v15).

**For simplicity and reproducibility, this repository assumes Conda/Mamba is required.** The provided `environment.yml` creates an isolated environment (similar to a `venv`) and pins **Python 3.9** (3.9.x) and installs the runtime package set used in the working setup.

---

## 1) Prerequisites

### Required (all users)
- macOS / Linux / Windows
- **Conda or Mamba** (required):
  - Mamba is faster; conda is fine.
- Git (optional but recommended)

### Optional (only if running Stage E: NCES enrichment)
Stage E uses Selenium + automated downloads.

Install separately:
- **Google Chrome** (stable channel)

Notes:
- We intentionally keep Chrome out of the Python environment file because it varies by OS and is typically managed by the system package manager.
- Selenium 4+ can often manage drivers automatically (Selenium Manager). If that fails on your system, you may need to install ChromeDriver manually.

---

## 2) Create the conda environment (required)

### A) Use the provided `environment.yml` (Python-focused)

Use the `environment.yml` provided in this repository (in the project root). This repository’s `environment.yml` is the source of truth for the runtime environment.

```yaml
name: aframr-runtime
channels:
  - conda-forge
dependencies:
  - python=3.9
  - pip
  - numpy=2.0.2
  - pandas=2.3.0
  - lxml
  - tqdm
  - pyyaml
  - pip:
      - -r requirements.runtime.base.txt
      - -r requirements.runtime.platform.txt
```

The runtime pip requirements are split into:
- `requirements.runtime.base.txt` (cross-platform runtime pins)
- `requirements.runtime.platform.txt` (platform-specific pins using markers; e.g., macOS-only `appnope`)

For reference/provenance, the original full working environment freeze (including notebook/dev packages) is archived at:
- `config/requirements.lock.full.txt`

### B) Create and activate the environment

From the repo root:

```bash
conda env create -f environment.yml
conda activate aframr-runtime
```

### C) Verify you are using the correct Python

After activation, confirm you are *not* using conda `base` and not using a project `venv`:

```bash
which python
python --version
python -c "import sys; print(sys.executable)"
conda info --envs
```

Expected:
- `python --version` shows **3.9.x**
- `sys.executable` path includes `.../envs/aframr-runtime/...`

To update an existing env after changing `environment.yml`:

```bash
conda env update -f environment.yml --prune
```

---

## 3) Sanity checks

Run a quick import check:

```bash
python -c "import pandas, numpy, bs4, lxml, requests, tqdm, yaml; print('core imports: OK')"
```

### Note for macOS users (LibreSSL warning)

If you see a warning like:
`urllib3 v2 only supports OpenSSL 1.1.1+ ... LibreSSL ...`

This usually indicates you are not using the conda environment’s Python (or your SSL libraries are coming from the wrong place). First re-run the verification commands in Section 2C. If you are in `aframr-runtime` and everything works, this warning can typically be ignored.

If you plan to run NCES enrichment (Stage E):

```bash
python -c "import selenium; print('selenium import: OK')"
```

---

## 4) Chrome + Selenium notes (Stage E only)

Stage E (`scripts/additional_institution_characteristics.py`) uses Selenium to load NCES pages and download exported spreadsheets.

Note: Stage E also requires `openpyxl`.

To add it to your existing environment without editing files:

```bash
conda activate aframr-runtime
conda install -c conda-forge openpyxl -y
```

Checklist:
- Install **Google Chrome** separately.
- Prefer Selenium 4+ (already included above). Selenium Manager may auto-handle drivers.
- If you hit driver errors:
  - Confirm Chrome is installed and can open normally.
  - Try updating Selenium.
  - As a last resort, install a matching ChromeDriver and ensure it’s on your PATH.

Operational tips:
- Use a dedicated download directory (do not use your default Downloads folder).
- Run Stage E on a **small test set first**, then batch for large runs.

More detail: `docs/06_nces_characteristics.md` (Stage E).

---

## Common installation issues

### Conda solve is slow or fails
- Try using mamba (faster solver), if available:

  ```bash
  mamba env create -f environment.yml
  ```

- Clear caches and retry:

  ```bash
  conda clean -a -y
  conda env create -f environment.yml
  ```

### Environment already exists / you want a clean rebuild

```bash
conda env remove -n aframr-runtime -y
conda env create -f environment.yml
conda activate aframr-runtime
```

### You are accidentally using `base` or a `venv`

Run the verification commands in Section 2C.

---

## 5) Next step

Once installation is complete, go to the root `README.md` and run the **Quickstart** (example run).
