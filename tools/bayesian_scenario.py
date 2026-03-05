"""bayesian_scenario tool — Bayesian investment simulation.

Three modes:
  init: Build priors from patent data + GDELT
  update: Bayesian update with user's private information
  simulate: Monte Carlo simulation with posterior parameters

All parameter evidence comes from patent/GDELT data.
Session state stored in simulation_logs table.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import time
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


# Royalty reference for prior construction
_ROYALTY_RATES = {
    "A61": ("製薬・バイオ", 3.0, 7.0, 25.0),
    "G06": ("ソフトウェア・IT", 1.0, 3.5, 10.0),
    "H01": ("電子部品・半導体", 1.5, 3.0, 7.0),
    "H04": ("通信", 1.0, 3.0, 8.0),
    "B60": ("自動車", 1.0, 3.0, 5.0),
    "C08": ("化学・素材", 2.0, 4.0, 8.0),
    "F01": ("機械・エンジン", 1.0, 3.0, 6.0),
    "G01": ("計測・センサー", 2.0, 4.0, 7.0),
    "C12": ("バイオテクノロジー", 3.0, 8.0, 20.0),
    "G16": ("ヘルスケアIT", 2.0, 5.0, 12.0),
}
_DEFAULT_RATE = ("その他", 1.0, 3.0, 7.0)


def _generate_session_id() -> str:
    return hashlib.md5(f"{time.time()}{random.random()}".encode()).hexdigest()[:12]


def _ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simulation_logs (
            session_id TEXT PRIMARY KEY,
            cluster_id TEXT,
            firm_id TEXT,
            timestamp TEXT DEFAULT (datetime('now')),
            params_json TEXT
        )
    """)


def _load_session(conn, session_id) -> dict | None:
    row = conn.execute(
        "SELECT params_json FROM simulation_logs WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if row and row["params_json"]:
        return json.loads(row["params_json"])
    return None


def _save_session(conn, session_id, cluster_id, firm_id, params):
    _ensure_table(conn)
    conn.execute(
        "INSERT OR REPLACE INTO simulation_logs (session_id, cluster_id, firm_id, params_json) "
        "VALUES (?, ?, ?, ?)",
        (session_id, cluster_id, firm_id, json.dumps(params, ensure_ascii=False)),
    )
    conn.commit()


def _bayesian_update_normal(prior_mean, prior_std, user_value, user_confidence):
    """Conjugate normal update."""
    if user_confidence <= 0:
        return prior_mean, prior_std
    prior_precision = 1.0 / max(prior_std ** 2, 1e-10)
    user_precision = user_confidence * prior_precision * 10
    post_precision = prior_precision + user_precision
    post_mean = (prior_precision * prior_mean + user_precision * user_value) / post_precision
    post_std = (1.0 / post_precision) ** 0.5
    return post_mean, post_std


def _bayesian_update_beta(prior_alpha, prior_beta, user_value, user_confidence):
    """Beta distribution update with pseudo-observations."""
    n_pseudo = int(user_confidence * 20)
    successes = int(user_value * n_pseudo)
    failures = n_pseudo - successes
    return prior_alpha + successes, prior_beta + failures


def _sample_normal(mean, std, n=10000):
    return [random.gauss(mean, std) for _ in range(n)]


def _sample_beta(alpha, beta_param, n=10000):
    return [random.betavariate(max(alpha, 0.01), max(beta_param, 0.01)) for _ in range(n)]


def _percentiles(samples, ps):
    s = sorted(samples)
    n = len(s)
    return {p: s[int(n * p / 100)] for p in ps}


def _init_mode(
    store: PatentStore,
    resolver: EntityResolver,
    conn,
    technology: str,
    firm_query: str | None,
    investment_cost: float,
    time_horizon_years: int,
) -> dict[str, Any]:
    """Build priors from patent data."""
    session_id = _generate_session_id()

    # Resolve cluster
    cluster_id = None
    cluster_label = technology
    cpc_class = ""

    if "_" in technology and len(technology) <= 40:
        row = conn.execute(
            "SELECT cluster_id, label, cpc_class FROM tech_clusters WHERE cluster_id = ?",
            (technology,),
        ).fetchone()
        if row:
            cluster_id = row["cluster_id"]
            cluster_label = row["label"]
            cpc_class = row["cpc_class"]

    if not cluster_id:
        row = conn.execute(
            "SELECT cluster_id, label, cpc_class FROM tech_clusters "
            "WHERE cpc_class LIKE ? || '%' OR label LIKE '%' || ? || '%' "
            "ORDER BY patent_count DESC LIMIT 1",
            (technology[:4].upper(), technology),
        ).fetchone()
        if row:
            cluster_id = row["cluster_id"]
            cluster_label = row["label"]
            cpc_class = row["cpc_class"]

    if not cluster_id:
        return {"error": f"No technology cluster found for: '{technology}'"}

    # Resolve firm if given
    firm_id = None
    if firm_query:
        resolved = resolver.resolve(firm_query, country_hint="JP")
        if resolved:
            firm_id = resolved.entity.canonical_id

    # --- Build priors ---

    # 1. Market growth rate from tech_cluster_momentum
    growth_data = conn.execute(
        "SELECT year, growth_rate FROM tech_cluster_momentum "
        "WHERE cluster_id = ? ORDER BY year",
        (cluster_id,),
    ).fetchall()

    if growth_data:
        rates = [r["growth_rate"] for r in growth_data if r["growth_rate"] is not None]
        gr_mean = sum(rates) / len(rates) if rates else 0.05
        gr_std = (sum((r - gr_mean) ** 2 for r in rates) / max(len(rates), 1)) ** 0.5 if len(rates) > 1 else 0.05
    else:
        gr_mean = 0.05
        gr_std = 0.05

    # New entrants count from startability_delta
    new_entrants = 0
    try:
        ent_row = conn.execute(
            "SELECT COUNT(DISTINCT firm_id) as cnt FROM startability_surface s2 "
            "WHERE cluster_id = ? AND year = (SELECT MAX(year) FROM startability_surface) "
            "AND score > 0.3 AND NOT EXISTS ("
            "  SELECT 1 FROM startability_surface s1 "
            "  WHERE s1.firm_id = s2.firm_id AND s1.cluster_id = s2.cluster_id "
            "  AND s1.year = s2.year - 2 AND s1.score > 0.3"
            ")",
            (cluster_id,),
        ).fetchone()
        new_entrants = ent_row["cnt"] if ent_row else 0
    except Exception:
        pass

    market_growth = {
        "distribution": "normal",
        "mean": round(gr_mean, 4),
        "std": round(max(gr_std, 0.02), 4),
        "evidence": (
            f"クラスタ{cluster_id}の出願数は年率{gr_mean:.1%}成長。"
            f"startability_deltaから{new_entrants}社が新規参入中。"
        ),
        "source_data": {
            "patent_filing_trend": [{"year": r["year"], "rate": r["growth_rate"]} for r in growth_data[-5:]],
            "new_entrants_count": new_entrants,
        },
    }

    # 2. Licensee count from startability_ranking
    potential_rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM startability_surface "
        "WHERE cluster_id = ? AND year = (SELECT MAX(year) FROM startability_surface WHERE cluster_id = ?) "
        "AND gate_open = 1",
        (cluster_id, cluster_id),
    ).fetchone()
    potential = potential_rows["cnt"] if potential_rows else 10

    high_fit_rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM startability_surface "
        "WHERE cluster_id = ? AND year = (SELECT MAX(year) FROM startability_surface WHERE cluster_id = ?) "
        "AND gate_open = 1 AND score > 0.5",
        (cluster_id, cluster_id),
    ).fetchone()
    high_fit = high_fit_rows["cnt"] if high_fit_rows else 5

    licensee_lambda = max(high_fit * 0.3, 1.0)  # ~30% of high-fit firms as realistic targets
    licensee_count = {
        "distribution": "poisson",
        "lambda": round(licensee_lambda, 1),
        "evidence": (
            f"startability_ranking上位でgate_open=trueの企業数が{potential}社。"
            f"うちstartability 0.5以上が{high_fit}社。"
        ),
        "source_data": {
            "potential_licensees": potential,
            "high_fit_licensees": high_fit,
        },
    }

    # 3. Royalty rate from industry mapping
    cpc3 = cpc_class[:3].upper() if cpc_class else ""
    rate_info = _ROYALTY_RATES.get(cpc3, _DEFAULT_RATE)
    industry, low, med, high = rate_info

    # Convert to beta distribution on [0, max/100]
    # mean = med/100, concentrate around median
    royalty_mean = med / 100.0
    royalty_alpha = 3.0
    royalty_beta = royalty_alpha * (1 / royalty_mean - 1) if royalty_mean > 0 else 3.0

    royalty_rate = {
        "distribution": "beta",
        "alpha": round(royalty_alpha, 2),
        "beta": round(royalty_beta, 2),
        "evidence": (
            f"CPC {cpc3}（{industry}）の業界参考レート: {low:.1f}-{high:.1f}%。"
            f"中央値{med:.1f}%。"
        ),
        "source_data": {
            "industry": industry,
            "reference_range": [low, high],
        },
    }

    # 4. Adoption probability
    # Based on momentum and cluster maturity
    mom_row = conn.execute(
        "SELECT growth_rate, acceleration FROM tech_cluster_momentum "
        "WHERE cluster_id = ? ORDER BY year DESC LIMIT 1",
        (cluster_id,),
    ).fetchone()
    momentum = (mom_row["growth_rate"] or 0) if mom_row else 0
    adopt_alpha = 3.0 + max(momentum * 10, 0)
    adopt_beta = 5.0 - min(momentum * 5, 3)

    adoption_prob = {
        "distribution": "beta",
        "alpha": round(max(adopt_alpha, 0.5), 2),
        "beta": round(max(adopt_beta, 0.5), 2),
        "evidence": (
            f"tech_trend_alertのmomentumスコア: {momentum:.2f}。"
            f"類似技術クラスタの推定ライセンス採用率。"
        ),
    }

    # 5. Technology obsolescence rate
    if momentum > 0.1:
        obs_lambda = 0.05  # slow obsolescence for growing tech
        est_years = 20.0
    elif momentum > 0:
        obs_lambda = 0.1
        est_years = 10.0
    else:
        obs_lambda = 0.15
        est_years = 7.0

    obsolescence = {
        "distribution": "exponential",
        "lambda": round(obs_lambda, 3),
        "evidence": (
            f"クラスタ{cluster_id}の成長率{momentum:.1%}から、"
            f"技術陳腐化までの推定年数: {est_years:.1f}年。"
        ),
    }

    # GDELT signal if available
    gdelt_signal = None
    if firm_id:
        try:
            gd_row = conn.execute(
                "SELECT investment_signal FROM gdelt_company_features "
                "WHERE firm_id = ? ORDER BY period_end DESC LIMIT 1",
                (firm_id,),
            ).fetchone()
            if gd_row:
                gdelt_signal = gd_row["investment_signal"]
                market_growth["source_data"]["gdelt_investment_signal"] = gdelt_signal
        except Exception:
            pass

    priors = {
        "market_growth_rate": market_growth,
        "licensee_count": licensee_count,
        "royalty_rate_pct": royalty_rate,
        "adoption_probability": adoption_prob,
        "technology_obsolescence_rate": obsolescence,
    }

    # Store session
    session_data = {
        "cluster_id": cluster_id,
        "cluster_label": cluster_label,
        "firm_id": firm_id,
        "investment_cost": investment_cost,
        "time_horizon_years": time_horizon_years,
        "priors": priors,
        "updates": [],
    }
    _save_session(conn, session_id, cluster_id, firm_id, session_data)

    guide = (
        f"上記は全て特許データとGDELTから導出した事前分布です。\n"
        f"追加の情報をお持ちですか？ 以下のパラメータを調整できます:\n\n"
        f"- market_growth_rate: 市場成長率（現在の推定: {gr_mean:.1%}±{gr_std:.1%}）\n"
        f"- licensee_count: 想定ライセンシー数（現在の推定: {licensee_lambda:.0f}社）\n"
        f"- royalty_rate_pct: ロイヤリティレート（現在の推定: {med:.1f}%）\n"
        f"- adoption_probability: 技術採用確率\n"
        f"- technology_obsolescence_rate: 技術陳腐化速度\n\n"
        f"update(parameter='market_growth_rate', user_value=0.3, user_confidence=0.8)\n"
        f"のように呼び出してください。"
    )

    return {
        "endpoint": "bayesian_scenario",
        "mode": "init",
        "session_id": session_id,
        "cluster_id": cluster_id,
        "cluster_label": cluster_label,
        "priors": priors,
        "guide": guide,
        "visualization_hint": {
            "recommended_chart": "parameter_dashboard",
            "title": "ベイジアンシナリオ: 事前分布",
            "axes": {"parameters": "priors", "distributions": "distribution"},
        },
    }


def _update_mode(conn, session_id, parameter, user_value, user_confidence) -> dict[str, Any]:
    """Update a parameter with user's private information."""
    session = _load_session(conn, session_id)
    if session is None:
        return {"error": f"Session not found: '{session_id}'"}

    priors = session["priors"]
    if parameter not in priors:
        return {"error": f"Unknown parameter: '{parameter}'",
                "available": list(priors.keys())}

    prior = priors[parameter]
    dist = prior["distribution"]

    if dist == "normal":
        old_mean, old_std = prior["mean"], prior["std"]
        new_mean, new_std = _bayesian_update_normal(old_mean, old_std, user_value, user_confidence)
        posterior = {"distribution": "normal", "mean": round(new_mean, 4), "std": round(new_std, 4)}

    elif dist == "beta":
        old_alpha, old_beta = prior["alpha"], prior["beta"]
        new_alpha, new_beta = _bayesian_update_beta(old_alpha, old_beta, user_value, user_confidence)
        posterior = {"distribution": "beta", "alpha": round(new_alpha, 2), "beta": round(new_beta, 2)}

    elif dist == "poisson":
        old_lambda = prior["lambda"]
        new_lambda = old_lambda * (1 - user_confidence) + user_value * user_confidence
        posterior = {"distribution": "poisson", "lambda": round(max(new_lambda, 0.1), 2)}

    elif dist == "exponential":
        old_lambda = prior["lambda"]
        new_lambda = old_lambda * (1 - user_confidence) + user_value * user_confidence
        posterior = {"distribution": "exponential", "lambda": round(max(new_lambda, 0.001), 4)}

    else:
        return {"error": f"Unsupported distribution type: '{dist}'"}

    # Update session
    priors[parameter] = {**prior, **posterior, "is_user_modified": True}
    session["priors"] = priors
    session["updates"].append({
        "parameter": parameter,
        "user_value": user_value,
        "user_confidence": user_confidence,
        "timestamp": time.time(),
    })
    _save_session(conn, session_id, session.get("cluster_id"), session.get("firm_id"), session)

    return {
        "endpoint": "bayesian_scenario",
        "mode": "update",
        "session_id": session_id,
        "parameter": parameter,
        "prior": {k: v for k, v in prior.items() if k not in ("source_data", "evidence")},
        "user_input": {"value": user_value, "confidence": user_confidence},
        "posterior": posterior,
        "current_params": {
            k: {
                "distribution": v["distribution"],
                "is_user_modified": v.get("is_user_modified", False),
            }
            for k, v in priors.items()
        },
    }


def _simulate_mode(conn, session_id) -> dict[str, Any]:
    """Run Monte Carlo simulation with current parameters."""
    session = _load_session(conn, session_id)
    if session is None:
        return {"error": f"Session not found: '{session_id}'"}

    priors = session["priors"]
    cost = session["investment_cost"]
    horizon = session["time_horizon_years"]
    N = 5000  # Monte Carlo samples

    random.seed(42)  # Reproducible

    # Sample from each distribution
    growth_samples = _sample_normal(
        priors["market_growth_rate"]["mean"],
        priors["market_growth_rate"]["std"], N
    )

    # Licensee count (Poisson → approximate with normal)
    lic_lambda = priors["licensee_count"]["lambda"]
    licensee_samples = [max(0, random.gauss(lic_lambda, lic_lambda ** 0.5)) for _ in range(N)]

    royalty_p = priors["royalty_rate_pct"]
    if royalty_p["distribution"] == "beta":
        royalty_samples = _sample_beta(royalty_p["alpha"], royalty_p["beta"], N)
    else:
        royalty_samples = [royalty_p.get("lambda", 0.03)] * N

    adopt_p = priors["adoption_probability"]
    if adopt_p["distribution"] == "beta":
        adopt_samples = _sample_beta(adopt_p["alpha"], adopt_p["beta"], N)
    else:
        adopt_samples = [0.3] * N

    obs_lambda = priors["technology_obsolescence_rate"]["lambda"]
    obs_samples = [random.expovariate(max(obs_lambda, 0.001)) for _ in range(N)]

    # Scale royalty to actual percentage (beta gives 0-1, we need %)
    royalty_ref = priors["royalty_rate_pct"].get("source_data", {}).get("reference_range", [1.0, 7.0])
    royalty_max = royalty_ref[1] if len(royalty_ref) > 1 else 7.0

    # Simulate NPV for each sample
    discount_rate = 0.08  # 8% discount rate
    npvs = []
    annual_cf_sum = [0.0] * horizon

    for i in range(N):
        growth = growth_samples[i]
        n_licensees = max(1, licensee_samples[i])
        royalty_pct = royalty_samples[i] * royalty_max  # Scale to actual %
        adopt = adopt_samples[i]
        obs_life = obs_samples[i]

        npv = -cost
        for year in range(1, horizon + 1):
            if year > obs_life:
                revenue = 0
            else:
                # Revenue = licensees × adoption × royalty × market_growth
                base_revenue = n_licensees * adopt * (royalty_pct / 100.0) * 10000  # 万円 scale
                market_factor = (1 + growth) ** year
                revenue = base_revenue * market_factor

            discounted = revenue / (1 + discount_rate) ** year
            npv += discounted
            annual_cf_sum[year - 1] += revenue / N

        npvs.append(npv)

    # NPV statistics
    npv_mean = sum(npvs) / N
    npv_sorted = sorted(npvs)
    npv_median = npv_sorted[N // 2]
    npv_std = (sum((x - npv_mean) ** 2 for x in npvs) / N) ** 0.5
    prob_positive = sum(1 for x in npvs if x > 0) / N
    prob_above_cost = sum(1 for x in npvs if x > cost) / N

    pcts = _percentiles(npvs, [5, 25, 50, 75, 95])

    # Annual cashflow
    annual_cashflow = []
    cumulative = -cost
    for yr in range(horizon):
        rev = annual_cf_sum[yr]
        net = rev - (cost / horizon * 0.1 if yr < 3 else 0)  # Small ongoing cost
        cumulative += net
        annual_cashflow.append({
            "year": yr + 1,
            "expected_revenue": round(rev, 1),
            "expected_cost": round(cost / horizon * 0.1, 1) if yr < 3 else 0,
            "net": round(net, 1),
            "cumulative": round(cumulative, 1),
        })

    # Breakeven analysis
    breakeven_year = None
    cum = -cost
    for yr, cf in enumerate(annual_cashflow):
        cum_check = cf["cumulative"]
        if cum_check >= 0 and breakeven_year is None:
            breakeven_year = yr + 1

    # Sensitivity analysis (tornado)
    base_npv = npv_mean
    sensitivity = []
    for param_name, samples in [
        ("market_growth_rate", growth_samples),
        ("licensee_count", licensee_samples),
        ("royalty_rate_pct", royalty_samples),
        ("adoption_probability", adopt_samples),
    ]:
        s_sorted = sorted(samples)
        low_val = s_sorted[int(N * 0.16)]  # -1σ
        high_val = s_sorted[int(N * 0.84)]  # +1σ
        # Quick estimate of NPV impact
        low_npvs = [n for n, s in zip(npvs, samples) if s <= low_val]
        high_npvs = [n for n, s in zip(npvs, samples) if s >= high_val]
        tornado_low = sum(low_npvs) / max(len(low_npvs), 1) if low_npvs else base_npv
        tornado_high = sum(high_npvs) / max(len(high_npvs), 1) if high_npvs else base_npv

        sensitivity.append({
            "parameter": param_name,
            "tornado_low": round(tornado_low, 1),
            "tornado_high": round(tornado_high, 1),
            "elasticity": round((tornado_high - tornado_low) / max(abs(base_npv), 1), 3),
        })

    sensitivity.sort(key=lambda x: abs(x["elasticity"]), reverse=True)

    # Assumptions log
    assumptions_log = []
    for pname, pdata in priors.items():
        entry = {
            "parameter": pname,
            "is_user_modified": pdata.get("is_user_modified", False),
            "distribution": pdata["distribution"],
        }
        if pdata["distribution"] == "normal":
            entry["mean"] = pdata["mean"]
        elif pdata["distribution"] == "beta":
            entry["alpha"] = pdata["alpha"]
            entry["beta"] = pdata["beta"]
        elif pdata["distribution"] == "poisson":
            entry["lambda"] = pdata["lambda"]
        assumptions_log.append(entry)

    # Save final params to simulation_logs
    session["simulation_result"] = {
        "npv_mean": npv_mean,
        "prob_positive": prob_positive,
        "timestamp": time.time(),
    }
    _save_session(conn, session_id, session.get("cluster_id"), session.get("firm_id"), session)

    return {
        "endpoint": "bayesian_scenario",
        "mode": "simulate",
        "session_id": session_id,
        "npv_distribution": {
            "mean": round(npv_mean, 1),
            "median": round(npv_median, 1),
            "std": round(npv_std, 1),
            "percentiles": {str(k): round(v, 1) for k, v in pcts.items()},
            "probability_positive": round(prob_positive, 3),
            "probability_above_cost": round(prob_above_cost, 3),
        },
        "annual_cashflow": annual_cashflow,
        "breakeven": {
            "expected_year": breakeven_year,
            "probability_within_horizon": round(prob_positive, 3),
            "best_case_year": 1 if pcts[5] > 0 else (breakeven_year or horizon),
            "worst_case_year": "期間内に回収不能" if pcts[95] < 0 else horizon,
        },
        "sensitivity": sensitivity,
        "assumptions_log": assumptions_log,
        "visualization_hint": {
            "recommended_charts": [
                {"type": "distribution", "data": "npv_distribution", "title": "NPV確率分布"},
                {"type": "waterfall", "data": "annual_cashflow", "title": "年次キャッシュフロー"},
                {"type": "tornado", "data": "sensitivity", "title": "感度分析"},
                {"type": "line_with_ci", "data": "annual_cashflow", "title": "累積回収曲線"},
            ],
        },
    }


def bayesian_scenario(
    store: PatentStore,
    resolver: EntityResolver,
    mode: str = "init",
    # init params
    technology: str | None = None,
    firm_query: str | None = None,
    investment_cost: float = 10000,
    time_horizon_years: int = 10,
    # update params
    session_id: str | None = None,
    parameter: str | None = None,
    user_value: float | None = None,
    user_confidence: float = 0.5,
) -> dict[str, Any]:
    """Bayesian patent investment simulation."""
    conn = store._conn()
    _ensure_table(conn)

    if mode == "init":
        if not technology:
            return {"error": "technology parameter is required for mode='init'"}
        return _init_mode(
            store, resolver, conn,
            technology=technology,
            firm_query=firm_query,
            investment_cost=investment_cost,
            time_horizon_years=time_horizon_years,
        )

    elif mode == "update":
        if not session_id or not parameter or user_value is None:
            return {"error": "session_id, parameter, and user_value are required for mode='update'"}
        return _update_mode(conn, session_id, parameter, user_value, user_confidence)

    elif mode == "simulate":
        if not session_id:
            return {"error": "session_id is required for mode='simulate'"}
        return _simulate_mode(conn, session_id)

    else:
        return {"error": f"Invalid mode: '{mode}'. Use 'init', 'update', or 'simulate'."}
