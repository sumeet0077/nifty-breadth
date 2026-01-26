# Nifty 500 Market Breadth Dashboard

This dashboard tracks the percentage of Nifty 500 stocks trading above their 200-day Simple Moving Average (SMA), a key indicator of long-term market breadth.

## Features
- **Historical Breadth**: Interactive chart showing market health from 2015 to Present.
- **Participation Counts**: View exact number of advancing/declining stocks.
- **Auto-Updates**: Powered by GitHub Actions to fetch fresh data daily after market close.

## How to Deploy (Completely Free)

### 1. Push to GitHub
1.  Initialize a git repository in this folder:
    ```bash
    git init
    git add .
    git commit -m "Initial commit"
    ```
2.  Create a new repository on GitHub (e.g., `nifty-breadth-dashboard`).
3.  Link and push:
    ```bash
    git remote add origin https://github.com/YOUR_USERNAME/nifty-breadth-dashboard.git
    git branch -M main
    git push -u origin main
    ```

### 2. Connect to Streamlit Cloud
1.  Go to [share.streamlit.io](https://share.streamlit.io/).
2.  Sign in with GitHub.
3.  Click **"New app"**.
4.  Select your repository (`nifty-breadth-dashboard`), branch (`main`), and main file (`app.py`).
5.  Click **"Deploy"**.

### 3. Automatic Updates
*   The `.github/workflows/daily_update.yml` file is configured to run every day at **6:00 PM IST (12:30 UTC)**.
*   It will download the latest data, update `market_breadth_history.csv`, and push it back to the repository.
*   Streamlit Cloud will detect the change and automatically update the dashboard.

## Local Setup
To run locally:
```bash
pip install -r requirements.txt
streamlit run app.py
```
