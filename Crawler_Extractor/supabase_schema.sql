-- Supabase schema for Pharmacogenetics Paper Explorer
-- Run via the Supabase SQL editor or `supabase db remote commit`.

create table if not exists public.papers (
    id uuid primary key,
    title text not null,
    source_uri text not null unique,
    pdf_md5 text,
    status text not null,
    level integer not null,
    attempts integer not null default 0,
    parent_id uuid references public.papers(id) on delete set null,
    seed_number integer,
    score double precision,
    reason text,
    model_name text,
    duration_ms integer,
    text_uri text,
    metadata jsonb,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.pgx_extractions (
    id bigserial primary key,
    paper_id uuid not null references public.papers(id) on delete cascade,
    sample_id text not null,
    gene text not null,
    allele text not null,
    rs_id text not null,
    medication text not null,
    outcome text not null,
    actionability text not null,
    cpic_recommendation text not null,
    source_context text not null,
    created_at timestamptz not null default timezone('utc', now())
);

create index if not exists pgx_extractions_paper_id_idx on public.pgx_extractions (paper_id);

create index if not exists papers_status_idx on public.papers (status);
create index if not exists papers_level_idx on public.papers (level, created_at);

create table if not exists public.gpapers (
    id uuid primary key,
    title text not null,
    source_uri text not null unique,
    pdf_md5 text,
    status text not null,
    gstatus text not null default 'P1',
    level integer not null,
    attempts integer not null default 0,
    parent_id uuid,
    seed_number integer,
    score double precision,
    reason text,
    model_name text,
    duration_ms integer,
    text_uri text,
    metadata jsonb,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists gpapers_gstatus_idx on public.gpapers (gstatus);
create index if not exists gpapers_level_idx on public.gpapers (level, created_at);

create table if not exists public.pgx_gextractions (
    id bigserial primary key,
    paper_id uuid not null references public.gpapers(id) on delete cascade,
    sample_id text not null,
    gene text not null,
    allele text not null,
    rs_id text not null,
    medication text not null,
    outcome text not null,
    actionability text not null,
    cpic_recommendation text not null,
    source_context text not null,
    created_at timestamptz not null default timezone('utc', now())
);

create index if not exists pgx_gextractions_paper_id_idx on public.pgx_gextractions (paper_id);

insert into public.gpapers (
    id,
    title,
    source_uri,
    pdf_md5,
    status,
    gstatus,
    level,
    attempts,
    parent_id,
    seed_number,
    score,
    reason,
    model_name,
    duration_ms,
    text_uri,
    metadata,
    created_at,
    updated_at
)
select
    p.id,
    p.title,
    p.source_uri,
    p.pdf_md5,
    p.status,
    'P1' as gstatus,
    p.level,
    p.attempts,
    p.parent_id,
    p.seed_number,
    p.score,
    p.reason,
    p.model_name,
    p.duration_ms,
    p.text_uri,
    p.metadata,
    p.created_at,
    p.updated_at
from public.papers p
where p.status in ('P1', 'P1Success', 'P1Failure', 'P1WIP')
on conflict (id) do update
set
    title = excluded.title,
    source_uri = excluded.source_uri,
    pdf_md5 = excluded.pdf_md5,
    status = excluded.status,
    gstatus = 'P1',
    level = excluded.level,
    attempts = excluded.attempts,
    parent_id = excluded.parent_id,
    seed_number = excluded.seed_number,
    score = excluded.score,
    reason = excluded.reason,
    model_name = excluded.model_name,
    duration_ms = excluded.duration_ms,
    text_uri = excluded.text_uri,
    metadata = excluded.metadata,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at;

alter table public.papers
    enable row level security;

create policy "select_papers" on public.papers
    for select
    using (true);

create policy "insert_papers" on public.papers
    for insert
    with check (true);

create policy "update_papers" on public.papers
    for update
    using (true)
    with check (true);

alter table public.gpapers
    enable row level security;

create policy "select_gpapers" on public.gpapers
    for select
    using (true);

create policy "insert_gpapers" on public.gpapers
    for insert
    with check (true);

create policy "update_gpapers" on public.gpapers
    for update
    using (true)
    with check (true);

alter table public.pgx_extractions
    enable row level security;

create policy "select_pgx_extractions" on public.pgx_extractions
    for select
    using (true);

create policy "insert_pgx_extractions" on public.pgx_extractions
    for insert
    with check (true);

alter table public.pgx_gextractions
    enable row level security;

create policy "select_pgx_gextractions" on public.pgx_gextractions
    for select
    using (true);

create policy "insert_pgx_gextractions" on public.pgx_gextractions
    for insert
    with check (true);

create or replace function public.stats_level_status(seed_filter integer default null)
returns table(status text, level integer, seed_number integer, total bigint)
language sql
stable
as $$
    select
        status,
        level,
        seed_number,
        count(*)::bigint as total
    from public.papers
    where seed_filter is null or seed_number = seed_filter
    group by status, level, seed_number
    order by status, level, seed_number;
$$;

create or replace function public.stats_seed_status()
returns table(seed_number integer, status text, total bigint)
language sql
stable
as $$
    select
        seed_number,
        status,
        count(*)::bigint as total
    from public.papers
    group by seed_number, status
    order by seed_number, status;
$$;

create or replace function public.stats_level_score(seed_filter integer default null)
returns table(level integer, score double precision, seed_number integer, total bigint)
language sql
stable
as $$
    select
        level,
        score,
        seed_number,
        count(*)::bigint as total
    from public.papers
    where score is not null
      and (seed_filter is null or seed_number = seed_filter)
    group by level, score, seed_number
    order by level, score, seed_number;
$$;
