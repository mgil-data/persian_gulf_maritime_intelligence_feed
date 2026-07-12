# vessels_ports_data_pipeline.py
#
# Migration applied for VesselAPI endpoint retirement (effective 2026-08-10 23:59 UTC):
#   - /v1/vessel/{id}/ownership   -> RETIRED. Ownership now read from the base
#                                    /v1/vessel/{id} record (Section 3).
#   - /v1/vessel/{id}/inspections -> RETIRED. Inspection rows now come from the
#                                    /v1/vessel/{id}/casualties response (Section 5).
# Unaffected: bounding-box (Sec.1), portevents (Sec.2), casualties (Sec.5).
#
# NOTE (verify against a live response before shipping):
#   1. That the base /v1/vessel/{id} record carries the ownership depth you need
#      (registered vs. beneficial owner, ISM manager).
#   2. The exact array keys in the casualties payload ("casualties", "inspections",
#      and whether deficiencies are nested or a third array).

import polars as pl
import requests
from transforms.api import transform, Output, LightweightOutput
from transforms.external.systems import external_systems, Source, ResolvedSource


@external_systems(
    hormuz_rest_api=Source(
        "<DATASET_RID>"
    )
)
@transform.using(
    raw_vessels_hormuz=Output(
        "<DATASET_RID>"
    ),
    raw_vessel_ownership=Output(
        "<DATASET_RID>"
    ),
    raw_port_events=Output(
        "<DATASET_RID>"
    ),
    raw_vessel_inspections=Output(
        "<DATASET_RID>"
    ),
    raw_vessel_casualties=Output(
        "<DATASET_RID>"
    )
)
def compute(
    hormuz_rest_api: ResolvedSource,
    raw_vessels_hormuz: LightweightOutput,
    raw_vessel_ownership: LightweightOutput,
    raw_port_events: LightweightOutput,
    raw_vessel_inspections: LightweightOutput,
    raw_vessel_casualties: LightweightOutput
):
    session = requests.Session()
    session.headers.update({"Authorization": "<API_KEY>"})
    base_url = "https://api.vesselapi.com"

    # 1. Bounding box  (unchanged)
    r = session.get(
        f"{base_url}/v1/location/vessels/bounding-box",
        params={
            "filter.latBottom": 24.9,
            "filter.latTop": 27.2,
            "filter.lonLeft": 55.4,
            "filter.lonRight": 57.0,
        }
    )
    print(f"Bounding box status: {r.status_code}")
    print(f"Response keys: {list(r.json().keys())}")
    print(f"Bounding box error: {r.text}")
    print(f"Vessels count: {len(r.json().get('vessels', []))}")

    vessels = r.json().get("vessels", [])
    df_vessels = pl.DataFrame(vessels) if vessels else pl.DataFrame()
    raw_vessels_hormuz.write_table(df_vessels)

    # 2. Port events  (unchanged)
    ports = [
        "IRBND",  # Bandar Abbas (Iran)
        "AEFJR",  # Fujairah (UAE)
        "AESHJ",  # Sharjah (UAE)
    ]
    all_events = []
    for port in ports:
        rp = session.get(f"{base_url}/v1/portevents/port/{port}")
        print(f"Puerto {port}: status {rp.status_code}")
        print(f"Respuesta: {rp.text[:200]}")
        if rp.status_code == 200:
            port_events = rp.json().get("portEvents", [])
            for event in port_events:
                event["port_code"] = port
            all_events.extend(port_events)

    df_events = pl.DataFrame(all_events) if all_events else pl.DataFrame()
    raw_port_events.write_table(df_events)

    # 3. Ownership  (MIGRATED: /ownership retired -> base /v1/vessel/{imo})
    #    The vessel ID lives in the URL, not the body, so we stamp `imo` on every
    #    record. That is the missing join key that broke the ownership join.
    #
    #    If the bounding-box vessels in Section 1 already carry the owner fields
    #    you need, you can DROP this loop entirely and build df_ownership by
    #    selecting the owner columns out of df_vessels (pure join, zero calls).
    imos = [v.get("imo") for v in vessels if v.get("imo")]
    ownership_list = []
    for imo in imos[:20]:
        r3 = session.get(f"{base_url}/v1/vessel/{imo}?filter.idType=imo")
        if r3.status_code == 200:
            record = r3.json().get("vessel", {}) or {}
            # Once you confirm the payload shape you can narrow to just the owner
            # block, e.g.:  record = record.get("owner", {}) or {}
            record["imo"] = imo          # <-- join key (fixes the missing-IMO gap)
            ownership_list.append(record)

    df_ownership = (
        pl.DataFrame(ownership_list) if ownership_list else pl.DataFrame()
    )
    raw_vessel_ownership.write_table(df_ownership)

    # 4. Inspections  (REMOVED: /inspections retired.)
    #    Inspection rows are now produced inside Section 5 from the casualties
    #    response. The raw_vessel_inspections output is written there.

    # 5. Casualties + Inspections  (both now come from the casualties endpoint)
    #    One call per IMO; split the response into two datasets at the raw layer
    #    so your downstream cleaning of each dataset keeps working untouched.
    casualties_list = []
    inspections_list = []
    for imo in imos[:20]:
        r5 = session.get(f"{base_url}/v1/vessel/{imo}/casualties?filter.idType=imo")
        if r5.status_code == 200:
            payload = r5.json() or {}
            for c in (payload.get("casualties") or []):
                c["imo"] = imo           # stamp join key (populated frame now
                casualties_list.append(c)  # matches the empty-schema fallback below)
            for i in (payload.get("inspections") or []):
                i["imo"] = imo
                inspections_list.append(i)

    # --- Casualties dataset (schema + empty-frame fallback unchanged) ---
    if casualties_list:
        df_casualties = pl.DataFrame(casualties_list)
    else:
        df_casualties = pl.DataFrame({
            "imo": pl.Series([], dtype=pl.Int64),
            "event_type": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "description": pl.Series([], dtype=pl.Utf8),
            "severity": pl.Series([], dtype=pl.Utf8),
            "pollution": pl.Series([], dtype=pl.Boolean),
            "vessel_status": pl.Series([], dtype=pl.Utf8),
        })
    raw_vessel_casualties.write_table(df_casualties)

    # --- Inspections dataset (fed from the same response) ---
    #     Explicit empty schema so a build with no rows doesn't produce a
    #     schemaless frame. Align these placeholder columns to the real payload.
    if inspections_list:
        df_inspections = pl.DataFrame(inspections_list)
    else:
        df_inspections = pl.DataFrame({
            "imo": pl.Series([], dtype=pl.Int64),
            "inspection_id": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "port": pl.Series([], dtype=pl.Utf8),
            "authority": pl.Series([], dtype=pl.Utf8),
            "deficiencies": pl.Series([], dtype=pl.Utf8),
            "detention": pl.Series([], dtype=pl.Boolean),
        })
    raw_vessel_inspections.write_table(df_inspections)
