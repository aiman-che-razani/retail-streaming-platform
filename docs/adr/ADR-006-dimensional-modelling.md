# ADR-006: Dimensional modelling — star schema, deterministic keys, hybrid SCD, inferred members

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, data-modeler

## Context

Analysts need fast, understandable answers to revenue, product, store and inventory questions (see `data-model.md` §6). Some dimension attributes change over time (product category and price, customer tier and location), and the business needs both "as it was when sold" and "as it is now" views. Events arrive out of order across topics (a sale can precede its product's creation event), and the whole warehouse must be rebuildable from RAW.

## Decision

- **Kimball star schema** in `ANALYTICS`: `FACT_SALES` (line grain), `FACT_INVENTORY` (movement grain), `FACT_INVENTORY_SNAPSHOT` (store × product × day), and `DIM_DATE`, `DIM_STORE`, `DIM_PRODUCT`, `DIM_CUSTOMER`. Grains are stated in table comments.
- **SCD types by attribute, not by table:** Type 2 where history has analytical value (product category/brand/price/cost/active; customer tier/location), Type 1 for corrections and consent (names, UOM, opt-in). `DIM_STORE` is Type 1 (region restatement is the business's preference).
- **Deterministic surrogate keys:** `MD5_NUMBER_LOWER64(natural_key || '|' || effective_from)`. The first version of every key has `effective_from = 1900-01-01`.
- **Reserved members:** `-1` Unknown, `-2` Not applicable (guest customer).
- **Late-arriving dimensions → inferred members** that share the first-version key, so they are "filled in" in place when the real record arrives.
- **Out-of-order dimension changes** → rebuild the version chain for affected natural keys and re-point affected fact rows, in one transaction (stored procedure).
- **Point-in-time joins** for fact loading.

## Alternatives

| Option | Why not chosen |
|---|---|
| One big table (denormalised) | Simple for BI, but every attribute change means rewriting history rows. SCD semantics get muddled and storage duplicates. We can still publish OBT-style *views* on top of the star if a BI tool wants them. |
| Data Vault 2.0 (hubs/links/satellites) | Excellent for integrating many sources with full auditability, but it adds a layer and many joins. With four sources, RAW already provides auditability. Overkill here. |
| Sequence-generated surrogate keys | Standard, but not reproducible: rebuilding from RAW assigns different keys, which breaks idempotent replays and makes out-of-order fixes much harder. |
| Map late-arriving facts to `-1` and fix later | Loses the natural key on the fact unless it is also stored, and requires a mass UPDATE of facts later. Inferred members avoid both. |
| Hold (park) facts until the dimension arrives | Delays revenue reporting indefinitely if the dimension event is lost; adds a stateful parking mechanism. |
| SCD2 on everything | Needless version churn (e.g., name typo fixes create versions), and analysts must understand history for attributes nobody asks about historically. |
| SCD Type 6 / mini-dimensions | Useful at scale for rapidly changing attributes (e.g., a customer's current-tier column on every version). Not needed yet; `is_current` rows give "as-is" views. |

## Consequences

**Positive**
- Replays and rebuilds are idempotent; keys never change for the same version.
- "As-was" and "as-is" analysis are both possible (join on key vs. join on natural key to `is_current`).
- Late-arriving products never block or drop revenue.

**Negative / accepted trade-offs**
- The SCD2 procedure is the most complex code in the project (rebuild and re-key). It needs strong SQL tests.
- Hash keys are wider-looking and meaningless to humans; natural keys are also kept on facts for debugging and re-keying.
- `DIM_STORE` Type 1 means historical "as-was region" reporting is impossible until it's converted to Type 2.
