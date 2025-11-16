# SalesOps Suite

Multi-agent SalesOps automation suite powered by AI ─ KPI analysis, anomaly detection, insight generation, and automated action recommendations. 📊🤖

## 🔧 Setup

### 1. Create & Activate Environment
This project uses a Conda environment defined in **environment.yml**.

```bash
conda activate salesops
```

### 1. Create environment from environment.yml

To recreate this project's environment exactly:
```
# create environment
mamba env create -f environment.yml
# or (if mamba not installed)
conda env create -f environment.yml
```
### 2. Activate the environment
```
conda activate salesops
```

### 3. Register Jupyter kernel (only first time)
```
python -m ipykernel install --user --name=salesops --display-name "Python (salesops)"
```

### 4. Select Interpreter in VS Code

Bottom-right → Click Python interpreter

Choose: Python (salesops) or conda: salesops

📁 Project Structure

```
salesops-suite/
  agents/          # agent code (KPI agent, anomaly agent, A2A agent, etc.)
  data/
      raw/         # original / downloaded datasets
      processed/   # cleaned, transformed snapshots for agents
  memory/          # session state, memory bank modules
  notebooks/       # data exploration & pipeline notebooks
  observability/   # logging, tracing, evaluation
  tests/           # unit tests
  tools/           # MCP tools, custom tools, utility functions
  .gitignore       # ignored files & folders
  environment.yml  # reproducible Conda environment
  LICENCE          # project licence
  README.md        # project documentation
  writeup.md       # project writeup & summary
  ```

## 🎯 Project Goal

Build a multi-agent system to automate SalesOps workflows:

KPI computation (MRR, revenue trends, conversion rate, etc.)

Anomaly detection in sales metrics

Insight & explanation generation

Action recommendations (assign leads, flag segments, etc.)

Multi-Agent orchestration (A2A Protocol)

Proper Sessions, Memory, Observability, Evaluation

This README will expand as the project progresses (Day 1 to Day 14).

📌 Status

Currently on Day 0 – Environment, Folder Structure & Setup Completed.
