# Portfolio Tracker

Streamlit app that tracks your DEGIRO portfolio with S&P 500 benchmarking, dividend tracking, and per-asset breakdowns.

## Setup

1. **Create virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials:**
   ```bash
   cp .env.example .env
   # Edit .env with your DEGIRO username and password
   ```

4. **Run locally:**
   ```bash
   streamlit run app.py
   ```

## Deployment (GCP Cloud Run)

The app deploys automatically via Cloud Build when changes are pushed to the `master` branch.

**Prerequisites:**
- Store DEGIRO credentials in GCP Secret Manager as `DEGIRO_USERNAME` and `DEGIRO_PASSWORD`
- Cloud Build trigger configured to watch this repo's `master` branch
- The `secret-manager-accesor` service account needs Secret Manager accessor permissions

**Manual deploy:**
```bash
gcloud builds submit --config cloudbuild.yaml
```

## Architecture

- `app.py` — Streamlit UI
- `degiro_client.py` — DEGIRO broker API integration
- `portfolio.py` — Portfolio analytics (value over time, benchmark comparison, breakdowns)
- `market_data.py` — Historical price data via yfinance
