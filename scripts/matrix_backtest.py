name: Matrix Backtest (Kelly + TE)

on:
  workflow_dispatch:
  schedule:
    - cron: "0 6 * * 1"   # every Monday 06:00 UTC

permissions:
  contents: write   # needed to commit the /reports/ update

jobs:
  matrix:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        kelly_scale: [0.25, 0.5, 1.0]
        min_edge: [0.00, 0.02, 0.05]

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas tabulate

      # Create enriched demo dataset on the fresh runner
      - name: Prepare enriched input (demo)
        run: |
          python scripts/prepare_backtest_input.py
          test -f data/raw/odds/sample_odds_enriched.csv || (echo "Enriched CSV missing" && exit 1)

      - name: Run backtest (matrix combo)
        env:
          PYTHONPATH: ./scripts:.
        run: |
          mkdir -p results
          python scripts/backtest_all.py \
            --dataset "data/raw/odds/sample_odds_enriched.csv" \
            --bands "[1.2,2.0]" \
            --min_edge ${{ matrix.min_edge }} \
            --staking "kelly" \
            --kelly_scale ${{ matrix.kelly_scale }} \
            --bankroll 1000 \
            --cfg_id 1 \
            --outdir results
          mv results/summary.csv results/summary_${{ matrix.kelly_scale }}_${{ matrix.min_edge }}.csv
          echo "---- summary_${{ matrix.kelly_scale }}_${{ matrix.min_edge }}.csv"
          cat results/summary_${{ matrix.kelly_scale }}_${{ matrix.min_edge }}.csv

      - name: Upload per-combo artifact
        uses: actions/upload-artifact@v4
        with:
          name: matrix-${{ matrix.kelly_scale }}-${{ matrix.min_edge }}
          path: results/*.csv
          retention-days: 7

  merge:
    needs: matrix
    runs-on: ubuntu-latest

    steps:
      - name: Checkout (to commit reports)
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas tabulate

      - name: Download all artifacts
        uses: actions/download-artifact@v4
        with:
          path: collected

      - name: Merge results and print summary
        run: |
          python - <<'PY'
          import pandas as pd, pathlib, os

          base = pathlib.Path('collected')
          files = list(base.rglob('summary_*.csv'))
          if not files:
              print("No per-combo summaries found.")
              open('matrix_summary.csv','w').write('')
              raise SystemExit(0)

          dfs = []
          for f in files:
              # filenames like summary_0.25_0.02.csv
              stem = f.stem
              _, k, e = stem.split('_')
              df = pd.read_csv(f)
              df['kelly_scale'] = float(k)
              df['min_edge'] = float(e)
              dfs.append(df)

          out = pd.concat(dfs, ignore_index=True)
          out = out[['kelly_scale','min_edge','n_bets','total_staked','pnl','roi','end_bankroll']]
          out.sort_values(['kelly_scale','min_edge'], inplace=True)
          out.to_csv('matrix_summary.csv', index=False)

          try:
              md = out.to_markdown(index=False)
              print(md)
              if os.environ.get('GITHUB_STEP_SUMMARY'):
                  with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f:
                      f.write("# 📊 Matrix Backtest Summary\n\n")
                      f.write(md + "\n")
          except Exception:
              print(out)
          PY

      - name: Generate /reports files
        run: |
          python scripts/matrix_report.py

      - name: Commit report to repo
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          git config user.name  "github-actions"
          git config user.email "actions@users.noreply.github.com"
          git add reports/matrix_summary_latest.csv reports/matrix_summary_latest.md
          git commit -m "chore: update matrix summary [skip ci]" || echo "No changes to commit"
          git push

      - name: Upload merged summary artifact
        uses: actions/upload-artifact@v4
        with:
          name: matrix-summary
          path: matrix_summary.csv
          retention-days: 7

