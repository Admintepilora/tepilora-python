# Tepilora SDK (Python)

SDK Python (sync + async) per Tepilora API v3.

**218 operazioni** in **23 namespace** auto-generati dal registry.

## Install

```bash
pip install Tepilora
```

Optional extras:
```bash
pip install 'Tepilora[arrow]'   # PyArrow per formati binari
pip install 'Tepilora[polars]'  # Polars DataFrame support
```

## Quick Start

```python
import Tepilora as T

client = T.TepiloraClient(api_key="YOUR_KEY")

# Typed endpoints (IDE autocomplete)
securities = client.securities.search(query="MSCI ETF", limit=10)
print(securities["totalCount"])

# Raw call
resp = client.call("securities.search", params={"query": "MSCI", "limit": 5})
print(resp.data)
```

## Async

```python
import asyncio
import Tepilora as T

async def main():
    async with T.AsyncTepiloraClient(api_key="YOUR_KEY") as client:
        data = await client.securities.search(query="MSCI", limit=10)
        print(data)

asyncio.run(main())
```

## Namespaces

| Namespace | Operazioni | Descrizione |
|-----------|------------|-------------|
| `securities` | 12 | Search, filter, history, facets, MiFID, fees |
| `news` | 7 | Search, latest, trending, details |
| `publications` | 5 | Research reports and publications |
| `portfolio` | 19 | CRUD, returns, attribution, optimization |
| `analytics` | 68 | Rolling metrics, ratios, risk, factors |
| `alerts` | 9 | Alert rules CRUD, evaluate, history |
| `macro` | 6 | Economic indicators, calendar |
| `stocks` | 9 | Technicals, screening, peers, signals |
| `bonds` | 7 | Analyze, screen, ladder, curve, spread |
| `options` | 6 | Pricing, Greeks, IV, strategies |
| `esg` | 5 | ESG scores, screening, comparison |
| `factors` | 3 | Fama-French, momentum, factor loading |
| `fh` | 7 | Fundamentals history, financials |
| `clients` | 8 | B2B client management |
| `profiling` | 10 | MiFID questionnaires, suitability |
| `billing` | 10 | Fee calculations, schedules, records |
| `documents` | 4 | Document parsing, classification |
| `alternatives` | 9 | Alternative investments |
| `queries` | 8 | Saved queries CRUD, execute |
| `search` | 1 | Global search |
| `data` | 1 | Raw data access |
| `exports` | 2 | Data export to file formats |

## Esempi per Namespace

### Securities

```python
# Search
results = client.securities.search(query="MSCI World", limit=20)

# Get details
details = client.securities.description(identifier="IE00B4L5Y983EURXMIL")

# Price history
history = client.securities.history(
    identifiers=["IE00B4L5Y983EURXMIL", "FR0010655712EURXPAR"],
    start_date="2024-01-01",
    limit=1000
)

# Filter by criteria
filtered = client.securities.filter(filters={"Currency": "EUR", "TepiloraType": "ETF"})

# Get facets for building filters
facets = client.securities.facets(fields=["Currency", "TepiloraType", "Country"])
```

### Portfolio

```python
# Create portfolio
portfolio = client.portfolio.create(
    name="My Portfolio",
    input_type="fixed_weights",
    input_data={"IE00B4L5Y983EURXMIL": 0.6, "FR0010655712EURXPAR": 0.4}
)

# Get returns
returns = client.portfolio.returns(
    id=portfolio["id"],
    start_date="2024-01-01",
    return_method="twr"
)

# Performance attribution
attribution = client.portfolio.attribution(id=portfolio["id"])

# Optimize
optimized = client.portfolio.optimize(
    id=portfolio["id"],
    objective="max_sharpe",
    constraints={"max_weight": 0.3}
)
```

### Analytics

```python
# List available functions
functions = client.analytics.list()

# Get function help
help_info = client.analytics.help("rolling_volatility")

# Calculate rolling volatility
vol = client.analytics.rolling_volatility(
    identifiers="IE00B4L5Y983EURXMIL",
    period=252,
    start_date="2023-01-01"
)

# Rolling Sharpe ratio
sharpe = client.analytics.rolling_sharpe(
    identifiers="IE00B4L5Y983EURXMIL",
    period=252,
    rf=0.02
)

# Factor regression
factors = client.analytics.factor_regression(
    identifiers="IE00B4L5Y983EURXMIL",
    model="FF5"
)
```

### News & Publications

```python
# Search news
news = client.news.search(query="bitcoin", limit=20)

# Latest news
latest = client.news.latest(limit=10)

# Trending topics
trending = client.news.trending(limit=50, finance_only=True)

# Search publications
pubs = client.publications.search(query="market outlook", limit=10)
```

### Alerts

```python
# Create alert
alert = client.alerts.create(
    name="Price Alert",
    condition={"type": "price_change", "threshold": 5.0},
    action={"type": "webhook", "url": "https://..."}
)

# List alerts
alerts = client.alerts.list(enabled=True)

# Evaluate manually
result = client.alerts.evaluate(rule_id=alert["id"])
```

### Bonds

```python
# Analyze bond
analysis = client.bonds.analyze(
    identifier="XS1234567890",
    price=98.5,
    settlement_date="2024-02-01"
)

# Screen bonds
bonds = client.bonds.screen(
    criteria={"min_yield": 4.0, "max_duration": 5.0},
    limit=50
)

# Get yield curve
curve = client.bonds.curve(currency="EUR", date="2024-01-15")
```

## Arrow/Binary Formats

```python
from Tepilora.arrow import read_ipc_stream

# Request Arrow format
resp = client.call_arrow_ipc_stream("securities.search", params={"query": "ETF", "limit": 1000})
table = read_ipc_stream(resp.content)
print(table.to_pandas())
```

## Module-Level API

```python
import Tepilora as T

# Configure globally
T.configure(api_key="YOUR_KEY")

# Use without client instance
T.analytics.rolling_volatility(identifiers="IE00B4L5Y983EURXMIL")
```

Or via environment variables:
```bash
export TEPILORA_API_KEY=your_key
export TEPILORA_BASE_URL=https://tepiloradata.com
```

## Error Handling

```python
from Tepilora.errors import TepiloraAPIError

try:
    data = client.securities.search(query="invalid")
except TepiloraAPIError as e:
    print(f"Error: {e.message}")
    print(f"Code: {e.code}")
```

## API Endpoints

- `POST /T-Api/v3` - Unified action router (all operations)
- `GET /T-Api/v3/health` - Health check
- `GET /T-Api/v3/pricing` - Pricing info
- `GET /T-Api/v3/logs/status` - Logs status

## Version

```python
import Tepilora
print(Tepilora.__version__)  # 0.3.0
```
