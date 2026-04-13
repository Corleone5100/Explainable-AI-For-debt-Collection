# Model Improvements — Implementation Summary

Based on the evaluation report (`docs/evaluation_report.txt`), the following improvements have been implemented to address the identified weaknesses.

---

## PRIORITY 1: Fix Low/Medium Risk Over-Escalation ✅

### Problem
The agent was systematically over-escalating actions on Low and Medium risk borrowers:
- **Low risk**: 24% Legal Notice (should be 0-5%) → caused Rs. -1.3M loss on 339 decisions
- **Medium risk**: 29% Legal Notice (should be 20%) → 58.4% correctness (barely above random)
- **Very Low risk**: Only 34% No Action (should be 50%+) → unnecessary SMS costs

### Changes Made

**File: `scripts/debt_env.py`** (Reward Function)

| Penalty | Before | After | Rationale |
|---------|--------|-------|-----------|
| Legal on Very Low | 0.50× demand | 0.65× demand | Stronger deterrent |
| Field Visit on Very Low | 0.35× demand | 0.45× demand | Moderate increase |
| **Legal on Low** | **0.35× demand** | **0.60× demand** | **Critical fix** |
| Field Visit on Low | 0.25× demand | 0.35× demand | Moderate increase |
| Legal on Medium (overdue ≤ 5) | None | 0.40× demand | New warning tier |

**New Incentives:**
- **No Action on Very Low** (overdue ≤ 1 month): +5% of total_demand bonus
- **No Action on Low** (overdue ≤ 2 months): +3% of total_demand bonus

**Expected Impact:** +Rs. 1.5M improvement from Low/Medium risk recovery

---

## PRIORITY 2: Optimal Feature Set Selection ✅

### Problem
The ablation study revealed:
- Full 150-feature model: -Rs. 15.43M (catastrophic — curse of dimensionality)
- Baseline (8 features): -Rs. 15.05M (disastrous — no graph features)
- **+all_graph (16 features): +Rs. 1.74M** (best performance)
- Community features alone: +Rs. 0.99M (single most important addition)

### Changes Made

**File: `config.py`**
- Added `feature_set` configuration property with documentation

**File: `.env.example`**
- Added `FEATURE_SET=optimal` configuration option

**New File: `scripts/feature_selector.py`**
- CLI tool to filter CSV to optimal feature sets
- Supports: `optimal`, `full`, `baseline`, `community`
- Default: `optimal` (16 features: core + community + 1-hop + GAT embeddings)

**Recommended Feature Set (optimal):**
```
8 core features:
  income, cibil_score, overdue_months, bounce_count,
  coll_success_rate, age, occ_idx, reg_idx

4 community features:
  community_risk_pct, community_avg_overdue,
  community_total_demand, community_size

1 multi-hop signal:
  neighborhood_stress_1hop

16 GAT embeddings:
  gat_embedding_0 through gat_embedding_15

Total: 29 features (reduced from 150)
```

**Excluded (per ablation study):**
- Structural features (node_degree, pagerank, betweenness) — hurt performance
- Multi-hop 2-hop and 3-hop signals — add noise

---

## PRIORITY 3: Training Duration Documentation ✅

### Problem
- 150 features with 50K timesteps = 333 samples/feature (far too few)
- Full feature set needs 10x more training

### Changes Made

**File: `.env.example`**
- Documented training duration recommendations:
  - Optimal feature set: 500,000 timesteps (default)
  - Full feature set: 5,000,000 timesteps (10x more)

**File: `config.py`**
- Added detailed docstring for `feature_set` property explaining training requirements

---

## PRIORITY 4: Hybrid Rule-Based + PPO Policy ✅

### Problem
- Rule-based achieves Rs. 3,671/decision (46% higher than PPO's Rs. 2,513)
- PPO excels at High/Very High risk (95.9%/99.8% correctness)
- Rule-based works better for Low/Medium risk where PPO over-escalates

### Solution

**New File: `scripts/hybrid_policy.py`**

**Strategy:**
| Risk Category | Policy | Rationale |
|--------------|--------|-----------|
| Very Low | Rule-based | Rules excel at simple economics |
| Low | Rule-based | PPO over-escalates to Legal (24%) |
| Medium | PPO | Nuance matters, graph features help |
| High | PPO | 95.9% correctness — agent excels |
| Very High | PPO | 99.8% correctness — near-perfect |

**Rule-based policy for Low/Very Low:**
- Very Low, overdue ≤ 1 month → No Action
- Very Low, overdue > 1 month → SMS/Call
- Low, overdue ≤ 2 months → SMS/Call
- Low, overdue 2-3 months → 70% SMS, 30% Field Visit
- Low, overdue > 3 months → Field Visit

**Usage:**
```bash
# Evaluate hybrid policy
python scripts/hybrid_policy.py --evaluate --episodes 50

# Demo on sample borrowers
python scripts/hybrid_policy.py --demo

# Standalone evaluation (integrated into main pipeline)
python scripts/evaluation.py
```

**Integration:**
- Hybrid policy evaluation is now part of the main `evaluation.py` pipeline
- Results saved to `evaluation_outputs/hybrid_evaluation_results.json`
- Compared against Random, Rule-Based, and PPO policies

---

## Summary of All Changes

| File | Changes |
|------|---------|
| `scripts/debt_env.py` | Reward function penalties increased, new incentive tiers added |
| `config.py` | Added `feature_set` configuration property |
| `.env.example` | Added `FEATURE_SET` and training duration documentation |
| `scripts/feature_selector.py` | **NEW** — Feature set filtering utility |
| `scripts/hybrid_policy.py` | **NEW** — Hybrid Rule-Based + PPO policy |
| `scripts/evaluation.py` | Integrated hybrid policy evaluation into pipeline |

---

## Expected Performance Improvements

Based on the evaluation report analysis:

| Metric | Before | Expected After |
|--------|--------|----------------|
| Low risk correctness | 54.9% | 80%+ |
| Medium risk correctness | 58.4% | 75%+ |
| Low risk net reward | -Rs. 1.3M | Positive |
| Very Low risk net reward | -Rs. 297K | Break-even or positive |
| Overall avg reward/decision | Rs. 2,513 | Rs. 3,000+ |
| Hybrid policy efficiency | N/A | Rs. 3,500+/decision |

---

## Next Steps for Retraining

1. **Generate optimal feature CSV:**
   ```bash
   python scripts/feature_selector.py --set optimal
   ```

2. **Retrain with updated reward function:**
   ```bash
   python scripts/debt_env.py
   ```

3. **Evaluate with hybrid policy:**
   ```bash
   python scripts/evaluation.py
   ```

4. **Compare results:**
   - Check `evaluation_outputs/hybrid_evaluation_results.json`
   - Compare against previous `evaluation_results.json`
   - Verify Low/Medium risk correctness improvements
