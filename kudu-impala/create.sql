-- Two-stage load:
--   1. hits_parquet — external Parquet table over data/hits/, same
--      105-column dirty-Parquet shape as ../impala/create.sql
--      (BIGINT epoch-seconds, INT days-since-epoch).
--   2. hits — native Kudu table with the primary-key columns moved to
--      the front (Kudu requires PK first), TIMESTAMP → UNIXTIME_MICROS,
--      DATE → DATE (Kudu 1.15+), HASH(WatchID) PARTITIONS 16.
--   3. UPSERT INTO hits SELECT <casts> FROM hits_parquet, converting
--      epoch-seconds → TIMESTAMP and epoch-days → DATE inline.
--   4. COMPUTE STATS hits so the planner has NDVs.
--   5. DROP hits_parquet — the staged Parquet is not the SUT; we
--      benchmark Kudu, not the external-table code path.
--
-- Idempotent: dropping both tables first means re-running create.sql
-- rebuilds cleanly. The five DROPs use IF EXISTS so a fresh cluster
-- doesn't error on the first invocation.
--
-- UPSERT vs INSERT: ClickHouse's PRIMARY KEY is a sort key, not a
-- uniqueness constraint. If any of the ~100M source rows collide on
-- (CounterID, EventDate, UserID, EventTime, WatchID), INSERT would
-- abort the whole load. UPSERT is idempotent — inserts if PK is new,
-- updates non-PK columns if not — and on a fresh table with no
-- duplicates it costs nothing extra. Safer default.

CREATE DATABASE IF NOT EXISTS clickbench;
USE clickbench;

DROP TABLE IF EXISTS hits;
DROP TABLE IF EXISTS hits_parquet;

CREATE EXTERNAL TABLE hits_parquet (
    WatchID bigint,
    JavaEnable smallint,
    Title string,
    GoodEvent smallint,
    EventTime bigint,
    EventDate int,
    CounterID int,
    ClientIP int,
    RegionID int,
    UserID bigint,
    CounterClass smallint,
    OS smallint,
    UserAgent smallint,
    URL string,
    Referer string,
    IsRefresh smallint,
    RefererCategoryID smallint,
    RefererRegionID int,
    URLCategoryID smallint,
    URLRegionID int,
    ResolutionWidth smallint,
    ResolutionHeight smallint,
    ResolutionDepth smallint,
    FlashMajor smallint,
    FlashMinor smallint,
    FlashMinor2 string,
    NetMajor smallint,
    NetMinor smallint,
    UserAgentMajor smallint,
    UserAgentMinor string,
    CookieEnable smallint,
    JavascriptEnable smallint,
    IsMobile smallint,
    MobilePhone smallint,
    MobilePhoneModel string,
    Params string,
    IPNetworkID int,
    TraficSourceID smallint,
    SearchEngineID smallint,
    SearchPhrase string,
    AdvEngineID smallint,
    IsArtifical smallint,
    WindowClientWidth smallint,
    WindowClientHeight smallint,
    ClientTimeZone smallint,
    ClientEventTime bigint,
    SilverlightVersion1 smallint,
    SilverlightVersion2 smallint,
    SilverlightVersion3 int,
    SilverlightVersion4 smallint,
    PageCharset string,
    CodeVersion int,
    IsLink smallint,
    IsDownload smallint,
    IsNotBounce smallint,
    FUniqID bigint,
    OriginalURL string,
    HID int,
    IsOldCounter smallint,
    IsEvent smallint,
    IsParameter smallint,
    DontCountHits smallint,
    WithHash smallint,
    HitColor string,
    LocalEventTime bigint,
    Age smallint,
    Sex smallint,
    Income smallint,
    Interests smallint,
    Robotness smallint,
    RemoteIP int,
    WindowName int,
    OpenerName int,
    HistoryLength smallint,
    BrowserLanguage string,
    BrowserCountry string,
    SocialNetwork string,
    SocialAction string,
    HTTPError smallint,
    SendTiming int,
    DNSTiming int,
    ConnectTiming int,
    ResponseStartTiming int,
    ResponseEndTiming int,
    FetchTiming int,
    SocialSourceNetworkID smallint,
    SocialSourcePage string,
    ParamPrice bigint,
    ParamOrderID string,
    ParamCurrency string,
    ParamCurrencyID smallint,
    OpenstatServiceName string,
    OpenstatCampaignID string,
    OpenstatAdID string,
    OpenstatSourceID string,
    UTMSource string,
    UTMMedium string,
    UTMCampaign string,
    UTMContent string,
    UTMTerm string,
    FromTag string,
    HasGCLID smallint,
    RefererHash bigint,
    URLHash bigint,
    CLID int
)
STORED AS PARQUET
LOCATION '/clickbench/hits';

REFRESH hits_parquet;

-- Kudu-native table. Column order differs from ClickHouse's canonical
-- schema: (CounterID, EventDate, UserID, EventTime, WatchID) are
-- pulled to the front because Kudu requires primary-key columns to
-- appear first in the CREATE TABLE column list. Every other column
-- stays in original ClickHouse order.
--
-- All columns are NOT NULL, matching the source Parquet (no NULLs in
-- the ClickBench dataset). Kudu allows NULL on non-PK columns, but
-- the ClickHouse contract has none, so keeping NOT NULL preserves
-- schema fidelity across the site.
--
-- COMPRESSION LZ4 on the six long, high-cardinality string columns
-- (Title, URL, Referer, Params, SearchPhrase, OriginalURL): Kudu's
-- default for string columns is DICT_ENCODING, which falls back to
-- plain (uncompressed) storage once a column's distinct-value count is
-- high — exactly these columns. That fallback is the main reason the
-- Kudu table is ~3x larger on disk than the equivalent Parquet table
-- (which snappy/zstd-compresses everything), and the larger footprint
-- is why full-column scans (COUNT ... LIKE '%...%', GROUP BY URL,
-- SELECT *) trail the Parquet variant. LZ4 is Kudu's cheapest codec
-- (decode ~GB/s) so it shrinks scan I/O with negligible CPU cost on
-- the hot re-scans. Numeric/timestamp columns are left at their
-- BIT_SHUFFLE default, which already applies LZ4 internally — stacking
-- COMPRESSION on top of bitshuffle is explicitly discouraged. Low-
-- cardinality strings (PageCharset, HitColor, browser/country codes)
-- keep plain DICT_ENCODING, which already compresses them well.
--
-- PARTITION BY HASH(WatchID) PARTITIONS 16: WatchID has ~100M
-- distinct values (highest cardinality of the 5 PK columns), so
-- hashing on it alone produces the most balanced spread across 16
-- tablets. Including all 5 PK cols in the hash adds no meaningful
-- entropy and costs planning-time evaluation on every scan. 16
-- partitions on a single tserver ≈ 6.25M rows per tablet, which
-- lands comfortably in Kudu's per-tablet sweet spot.
CREATE TABLE hits (
    CounterID int NOT NULL,
    EventDate date NOT NULL,
    UserID bigint NOT NULL,
    EventTime timestamp NOT NULL,
    WatchID bigint NOT NULL,
    JavaEnable smallint NOT NULL,
    Title string NOT NULL COMPRESSION LZ4,
    GoodEvent smallint NOT NULL,
    ClientIP int NOT NULL,
    RegionID int NOT NULL,
    CounterClass smallint NOT NULL,
    OS smallint NOT NULL,
    UserAgent smallint NOT NULL,
    URL string NOT NULL COMPRESSION LZ4,
    Referer string NOT NULL COMPRESSION LZ4,
    IsRefresh smallint NOT NULL,
    RefererCategoryID smallint NOT NULL,
    RefererRegionID int NOT NULL,
    URLCategoryID smallint NOT NULL,
    URLRegionID int NOT NULL,
    ResolutionWidth smallint NOT NULL,
    ResolutionHeight smallint NOT NULL,
    ResolutionDepth smallint NOT NULL,
    FlashMajor smallint NOT NULL,
    FlashMinor smallint NOT NULL,
    FlashMinor2 string NOT NULL,
    NetMajor smallint NOT NULL,
    NetMinor smallint NOT NULL,
    UserAgentMajor smallint NOT NULL,
    UserAgentMinor string NOT NULL,
    CookieEnable smallint NOT NULL,
    JavascriptEnable smallint NOT NULL,
    IsMobile smallint NOT NULL,
    MobilePhone smallint NOT NULL,
    MobilePhoneModel string NOT NULL,
    Params string NOT NULL COMPRESSION LZ4,
    IPNetworkID int NOT NULL,
    TraficSourceID smallint NOT NULL,
    SearchEngineID smallint NOT NULL,
    SearchPhrase string NOT NULL COMPRESSION LZ4,
    AdvEngineID smallint NOT NULL,
    IsArtifical smallint NOT NULL,
    WindowClientWidth smallint NOT NULL,
    WindowClientHeight smallint NOT NULL,
    ClientTimeZone smallint NOT NULL,
    ClientEventTime timestamp NOT NULL,
    SilverlightVersion1 smallint NOT NULL,
    SilverlightVersion2 smallint NOT NULL,
    SilverlightVersion3 int NOT NULL,
    SilverlightVersion4 smallint NOT NULL,
    PageCharset string NOT NULL,
    CodeVersion int NOT NULL,
    IsLink smallint NOT NULL,
    IsDownload smallint NOT NULL,
    IsNotBounce smallint NOT NULL,
    FUniqID bigint NOT NULL,
    OriginalURL string NOT NULL COMPRESSION LZ4,
    HID int NOT NULL,
    IsOldCounter smallint NOT NULL,
    IsEvent smallint NOT NULL,
    IsParameter smallint NOT NULL,
    DontCountHits smallint NOT NULL,
    WithHash smallint NOT NULL,
    HitColor string NOT NULL,
    LocalEventTime timestamp NOT NULL,
    Age smallint NOT NULL,
    Sex smallint NOT NULL,
    Income smallint NOT NULL,
    Interests smallint NOT NULL,
    Robotness smallint NOT NULL,
    RemoteIP int NOT NULL,
    WindowName int NOT NULL,
    OpenerName int NOT NULL,
    HistoryLength smallint NOT NULL,
    BrowserLanguage string NOT NULL,
    BrowserCountry string NOT NULL,
    SocialNetwork string NOT NULL,
    SocialAction string NOT NULL,
    HTTPError smallint NOT NULL,
    SendTiming int NOT NULL,
    DNSTiming int NOT NULL,
    ConnectTiming int NOT NULL,
    ResponseStartTiming int NOT NULL,
    ResponseEndTiming int NOT NULL,
    FetchTiming int NOT NULL,
    SocialSourceNetworkID smallint NOT NULL,
    SocialSourcePage string NOT NULL,
    ParamPrice bigint NOT NULL,
    ParamOrderID string NOT NULL,
    ParamCurrency string NOT NULL,
    ParamCurrencyID smallint NOT NULL,
    OpenstatServiceName string NOT NULL,
    OpenstatCampaignID string NOT NULL,
    OpenstatAdID string NOT NULL,
    OpenstatSourceID string NOT NULL,
    UTMSource string NOT NULL,
    UTMMedium string NOT NULL,
    UTMCampaign string NOT NULL,
    UTMContent string NOT NULL,
    UTMTerm string NOT NULL,
    FromTag string NOT NULL,
    HasGCLID smallint NOT NULL,
    RefererHash bigint NOT NULL,
    URLHash bigint NOT NULL,
    CLID int NOT NULL,
    PRIMARY KEY (CounterID, EventDate, UserID, EventTime, WatchID)
)
PARTITION BY HASH (WatchID) PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses' = 'kudu-master-1:7051');

-- Stage the Parquet rows into Kudu. Type conversions inline:
--   * EventDate      INT days-since-epoch → DATE
--   * EventTime      BIGINT epoch-seconds → TIMESTAMP (Kudu stores as
--                    UNIXTIME_MICROS; Impala handles the conversion)
--   * ClientEventTime, LocalEventTime: same as EventTime.
--
-- Everything else passes through unchanged. Column order in the
-- SELECT list mirrors the target table's declared order — Impala
-- matches positionally, not by name.
--
-- MEM_LIMIT + /* +noclustered */:
--
-- Since Impala 3.0, INSERT/UPSERT into a Kudu table implicitly adds
-- a SORT_NODE before the Kudu sink to write rows in primary-key
-- order — the theory being that ordered writes are cheaper for
-- Kudu's tservers. On a 32 GB c6a.4xlarge this is fatal: the SORT
-- alone reserves ~14 GB (matching what the query profile shows in
-- the "Memory limit exceeded" failure), and Impala's per-query
-- reservation ceiling refuses to allocate a 2 MB row batch on top.
--
-- /* +noclustered */ tells Impala to skip the pre-sort and stream
-- rows straight to KuduTableSink. On a single-tserver cluster with
-- 16 hash-partitioned tablets the ordering benefit is small anyway
-- (Kudu will sort within each tablet's MRS regardless), and the
-- memory saving is what makes this run possible on c6a.4xlarge.
-- On the 64 GB laptop the default clustered insert fit and this
-- hint would have been unnecessary — the hint is safe there too,
-- so it's the right default going forward.
--
-- SET MEM_LIMIT=26g: belt-and-braces. Impala auto-computes a per-
-- query mem_limit based on available RAM; on c6a.4xlarge with the
-- coordinator+executor combined in one impalad-1, auto-detection
-- can under-provision. 26g gives headroom above the ~20g the
-- streaming UPSERT actually needs, well below the 32g box total.
SET MEM_LIMIT=26g;
UPSERT /* +noclustered */ INTO hits
SELECT
    CounterID,
    days_add(DATE '1970-01-01', EventDate)          AS EventDate,
    UserID,
    CAST(from_unixtime(EventTime) AS TIMESTAMP)     AS EventTime,
    WatchID,
    JavaEnable, Title, GoodEvent, ClientIP, RegionID,
    CounterClass, OS, UserAgent, URL, Referer,
    IsRefresh, RefererCategoryID, RefererRegionID,
    URLCategoryID, URLRegionID,
    ResolutionWidth, ResolutionHeight, ResolutionDepth,
    FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor,
    UserAgentMajor, UserAgentMinor,
    CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    MobilePhoneModel, Params, IPNetworkID,
    TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID,
    IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone,
    CAST(from_unixtime(ClientEventTime) AS TIMESTAMP) AS ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3,
    SilverlightVersion4, PageCharset, CodeVersion,
    IsLink, IsDownload, IsNotBounce, FUniqID,
    OriginalURL, HID, IsOldCounter, IsEvent, IsParameter,
    DontCountHits, WithHash, HitColor,
    CAST(from_unixtime(LocalEventTime) AS TIMESTAMP) AS LocalEventTime,
    Age, Sex, Income, Interests, Robotness, RemoteIP,
    WindowName, OpenerName, HistoryLength,
    BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction,
    HTTPError, SendTiming, DNSTiming, ConnectTiming,
    ResponseStartTiming, ResponseEndTiming, FetchTiming,
    SocialSourceNetworkID, SocialSourcePage,
    ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID,
    OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag,
    HasGCLID, RefererHash, URLHash, CLID
FROM hits_parquet;

-- Populate planner stats. Without this the first cold query pays the
-- stats-collection wall-clock as if it were query work, and joins
-- (there aren't any here, but future variants may add them) plan as
-- full broadcasts.
COMPUTE STATS hits;

-- Free the staged Parquet table — it's not the SUT and its data
-- file is deleted by ./load after this script completes.
DROP TABLE hits_parquet;
