name: Recheck Available Domains

on:
  schedule:
    - cron: '0 * * * *'  # every hour
  workflow_dispatch:

jobs:
  recheck-available-domains:
    runs-on: ubuntu-latest

    env:
      SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
      SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
      DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}

    steps:
      - name: Checkout repo
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'  # ✅ This must be 3.10 (not 3.1)

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install dnspython

      - name: Run recheck script
        run: python recheck_available_domains.py
