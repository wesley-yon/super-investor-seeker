-- 13F Super Investor Seeker — Insider Activity
-- Illustrative PostgreSQL schema. Adapt names, ID types, migrations, and ORM
-- conventions to the existing repository. This file is a logical reference,
-- not a command to introduce a second schema or replace the current ORM.

create table if not exists insider_filing (
    id uuid primary key,
    accession_number text not null unique,
    base_form_type text not null check (base_form_type in ('3', '4', '5')),
    form_type text not null check (form_type in ('3', '3/A', '4', '4/A', '5', '5/A')),
    is_amendment boolean not null default false,

    issuer_cik text not null,
    issuer_name_as_filed text not null,
    issuer_trading_symbol_as_filed text,
    foreign_trading_symbol_as_filed text,
    normalized_security_id uuid,

    schema_version text,
    period_of_report date,
    filing_date date not null,
    accepted_at timestamptz,
    original_submission_date date,

    aff10b5_one boolean,
    not_subject_to_section16 boolean,
    no_securities_owned boolean,
    form3_holdings_reported boolean,
    form4_transactions_reported boolean,
    remarks text,

    source_index_url text not null,
    source_document_url text not null,
    raw_xml_storage_key text,
    raw_xml_sha256 text,

    parse_status text not null default 'pending'
        check (parse_status in ('pending', 'parsed', 'warning', 'failed')),
    parser_version text not null,
    parse_error text,

    amends_filing_id uuid references insider_filing(id),
    amendment_match_confidence text
        check (amendment_match_confidence in ('high', 'medium', 'low', 'unresolved')),
    is_current_effective_version boolean not null default true,

    ingested_at timestamptz not null default now(),
    reprocessed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists insider_filing_issuer_date_idx
    on insider_filing (issuer_cik, filing_date desc);

create index if not exists insider_filing_accepted_idx
    on insider_filing (accepted_at desc);

create index if not exists insider_filing_effective_idx
    on insider_filing (issuer_cik, is_current_effective_version, period_of_report desc);

create table if not exists insider_owner (
    id uuid primary key,
    reporting_owner_cik text unique,
    normalized_name text not null,
    display_name text not null,
    is_entity boolean,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists insider_owner_name_idx
    on insider_owner (normalized_name);

create table if not exists insider_filing_owner (
    filing_id uuid not null references insider_filing(id) on delete cascade,
    owner_id uuid not null references insider_owner(id),
    owner_order integer not null,
    is_director boolean not null default false,
    is_officer boolean not null default false,
    is_ten_percent_owner boolean not null default false,
    is_other boolean not null default false,
    officer_title text,
    other_text text,
    address_json jsonb,
    primary key (filing_id, owner_id)
);

create index if not exists insider_filing_owner_owner_idx
    on insider_filing_owner (owner_id, filing_id);

create table if not exists insider_transaction (
    id uuid primary key,
    filing_id uuid not null references insider_filing(id) on delete cascade,
    source_table text not null check (source_table in ('non_derivative', 'derivative')),
    source_row_index integer not null,
    source_surrogate_key text,

    security_title_as_filed text not null,
    normalized_security_id uuid,
    transaction_date date not null,
    deemed_execution_date date,
    transaction_form_type text,
    transaction_code text,
    equity_swap_involved boolean,
    transaction_timeliness text check (transaction_timeliness in ('E', 'L') or transaction_timeliness is null),

    shares numeric(30, 8),
    price_per_share numeric(30, 8),
    reported_total_value numeric(30, 8),
    acquired_disposed_code text not null check (acquired_disposed_code in ('A', 'D')),
    post_transaction_shares numeric(30, 8),
    post_transaction_value numeric(30, 8),
    direct_indirect_ownership text not null check (direct_indirect_ownership in ('D', 'I')),
    nature_of_ownership text,

    conversion_or_exercise_price numeric(30, 8),
    exercise_date date,
    expiration_date date,
    underlying_security_title text,
    underlying_security_id uuid,
    underlying_shares numeric(30, 8),
    underlying_value numeric(30, 8),

    normalized_category text not null,
    is_meaningful_ps boolean not null default false,
    calculated_value numeric(30, 8),
    value_method text not null
        check (value_method in ('reported_total', 'calculated_shares_times_price', 'unavailable', 'not_applicable')),
    plan_status text not null
        check (plan_status in ('filing_marked', 'footnote_confirmed', 'not_marked', 'unknown')),

    owner_group_key text not null,
    display_group_key text not null,
    is_superseded boolean not null default false,
    raw_row_json jsonb not null,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    unique (filing_id, source_table, source_row_index)
);

create index if not exists insider_transaction_issuer_lookup_idx
    on insider_transaction (normalized_security_id, transaction_date desc);

create index if not exists insider_transaction_filing_idx
    on insider_transaction (filing_id, source_row_index);

create index if not exists insider_transaction_owner_group_idx
    on insider_transaction (owner_group_key, transaction_date desc);

create index if not exists insider_transaction_code_date_idx
    on insider_transaction (transaction_code, transaction_date desc)
    where is_superseded = false;

create index if not exists insider_transaction_ps_idx
    on insider_transaction (normalized_security_id, transaction_date desc, transaction_code)
    where is_meaningful_ps = true and is_superseded = false;

create table if not exists insider_holding (
    id uuid primary key,
    filing_id uuid not null references insider_filing(id) on delete cascade,
    source_table text not null check (source_table in ('non_derivative', 'derivative')),
    source_row_index integer not null,
    source_surrogate_key text,

    security_title_as_filed text not null,
    normalized_security_id uuid,
    transaction_form_type text,
    shares_owned numeric(30, 8),
    value_owned numeric(30, 8),
    direct_indirect_ownership text not null check (direct_indirect_ownership in ('D', 'I')),
    nature_of_ownership text,

    conversion_or_exercise_price numeric(30, 8),
    exercise_date date,
    expiration_date date,
    underlying_security_title text,
    underlying_security_id uuid,
    underlying_shares numeric(30, 8),
    underlying_value numeric(30, 8),

    owner_group_key text not null,
    is_superseded boolean not null default false,
    raw_row_json jsonb not null,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    unique (filing_id, source_table, source_row_index)
);

create index if not exists insider_holding_security_idx
    on insider_holding (normalized_security_id, filing_id);

create table if not exists insider_footnote (
    filing_id uuid not null references insider_filing(id) on delete cascade,
    footnote_id text not null,
    footnote_text text not null,
    primary key (filing_id, footnote_id)
);

create table if not exists insider_field_footnote_link (
    id uuid primary key,
    filing_id uuid not null references insider_filing(id) on delete cascade,
    entity_type text not null check (entity_type in ('filing', 'transaction', 'holding')),
    transaction_id uuid references insider_transaction(id) on delete cascade,
    holding_id uuid references insider_holding(id) on delete cascade,
    field_name text not null,
    footnote_id text not null,
    foreign key (filing_id, footnote_id)
        references insider_footnote(filing_id, footnote_id)
        on delete cascade,
    check (
        (entity_type = 'filing' and transaction_id is null and holding_id is null)
        or (entity_type = 'transaction' and transaction_id is not null and holding_id is null)
        or (entity_type = 'holding' and transaction_id is null and holding_id is not null)
    )
);

create index if not exists insider_field_footnote_transaction_idx
    on insider_field_footnote_link (transaction_id)
    where transaction_id is not null;

create index if not exists insider_field_footnote_holding_idx
    on insider_field_footnote_link (holding_id)
    where holding_id is not null;

create table if not exists insider_owner_signature (
    id uuid primary key,
    filing_id uuid not null references insider_filing(id) on delete cascade,
    signature_order integer not null,
    signature_name text not null,
    signature_date date not null,
    unique (filing_id, signature_order)
);

-- Optional derived latest-position table. Every row must be reproducible from
-- source filing observations and should be rebuilt after amendments/reparses.
create table if not exists insider_latest_position (
    id uuid primary key,
    issuer_cik text not null,
    normalized_security_id uuid not null,
    owner_group_key text not null,
    ownership_bucket_key text not null,
    direct_indirect_ownership text not null check (direct_indirect_ownership in ('D', 'I')),
    nature_of_ownership text,
    shares numeric(30, 8),
    value numeric(30, 8),
    observed_transaction_date date,
    observed_filing_date date not null,
    source_filing_id uuid not null references insider_filing(id),
    source_transaction_id uuid references insider_transaction(id),
    source_holding_id uuid references insider_holding(id),
    contains_indirect_ownership boolean not null default false,
    updated_at timestamptz not null default now(),
    unique (issuer_cik, normalized_security_id, owner_group_key, ownership_bucket_key)
);

create index if not exists insider_latest_position_ranking_idx
    on insider_latest_position (normalized_security_id, shares desc nulls last);
