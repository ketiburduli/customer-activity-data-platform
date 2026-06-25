# Sample data dictionary

Four **newline-delimited JSON** files (one JSON object per line), plus this dictionary.
They represent ~3 days of one tenant's activity (`tenant-01`), drawn from two source
systems. Volumes are tiny by design — treat the *shape* as faithful to production, not
the scale.

> **Conventions**
> - **Money** fields are **strings** carrying an exact `Decimal(38,18)` value. Parse them
>   as exact decimals — never floats. Example: `"0.450000000000000000"`.
> - **Times** are ISO-8601 UTC (`...Z`).
> - **`email_sha256`** is the SHA-256 of the lowercased email. It is the only reliable
>   join key between the two source systems.
> - **Sample period:** `2026-05-30` → `2026-06-01` (inclusive).

The two sources are shaped differently and overlap. Conforming them to one contract is
the point of the exercise (see Part A / Part B).

---

## `pam_customers.json` — PAM (customer system)
Grain: one row per PAM account.

| Field | Type | Req | Notes |
|---|---|---|---|
| `pam_customer_id` | string | yes | Source key in the PAM system (e.g. `TG-100482`). |
| `tenant` | string | yes | Operator the account belongs to (`tenant-01`). |
| `full_name` | string | yes | KYC name. |
| `email_sha256` | string (hex) | yes | Join key to `app_customers`. |
| `country` | string (ISO-2) | yes | Registration country. |
| `registered_at` | timestamp | yes | Account creation time. |

## `app_customers.json` — Activity platform
Grain: one row per activity-platform account.

| Field | Type | Req | Notes |
|---|---|---|---|
| `app_customer_id` | string | yes | Source key in the activity platform (e.g. `PK-55021`). |
| `username` | string | yes | Display handle. |
| `email_sha256` | string (hex) | yes | Join key to `pam_customers`. |
| `created_at` | timestamp | yes | Account creation time. |

## `activity_events.json` — Activity platform
Grain: one row per customer per activity event.

| Field | Type | Req | Notes |
|---|---|---|---|
| `event_id` | string | yes | Event key. `event_id` + `app_customer_id` is the natural key. |
| `app_customer_id` | string | yes | Who the event belongs to (links to `app_customers`). |
| `tenant` | string | yes | `tenant-01`. |
| `event_type` | string | yes | Activity category (`table`, `slots`, `live`). Not needed for the KPIs. |
| `event_time` | timestamp | yes | When the event happened. May arrive late / out of order. |
| `net_result_usdt` | string decimal | yes | Customer net for the event. Negative = loss. |
| `platform_fee_usdt` | string decimal | yes | Operator commission. **Must be `>= 0`.** |

## `customer_sessions.json` — PAM (customer system)
Grain: one row per login session.

| Field | Type | Req | Notes |
|---|---|---|---|
| `session_id` | string | yes | Session key. |
| `pam_customer_id` | string | yes | Whose session (links to `pam_customers`). |
| `login_at` | timestamp | yes | Session start. |
| `logout_at` | timestamp | yes | Session end. |
| `device` | string | yes | `ios` / `android` / `web`. |
| `ip_region` | string | yes | Coarse region code. |

---

## Planted edge cases (handle them; do not pre-clean the inputs)

These are intentional. Your pipeline should deal with them per the brief rather than
you editing the files:

- **Duplicates** — some `activity_events` lines repeat the same `event_id` +
  `app_customer_id` (idempotency).
- **Out-of-order** — some events have an `event_time` earlier than rows that appear
  before them in the file (ordering / replay).
- **Malformed money** — at least one event has a non-numeric money string.
- **Missing required key** — at least one event is missing `event_time`.
- **Contract violation** — at least one event has a negative `platform_fee_usdt`.
- **Identity: one person, two PAM ids** — the same `email_sha256` appears under two
  different `pam_customer_id`s. Both must resolve to one canonical customer.
- **Identity: app-only account** — at least one `app_customer_id` has activity but no
  matching PAM account (no shared `email_sha256`).

There is also a PAM account with a session but no activity, and an active customer with
no session — both are legitimate, not errors.
