import os
import re
from pathlib import Path

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut
import folium

# ========= CONFIG =========
# Point these at your cleaned files
CSV_PATH = "IOS_Tenant_Targets_cleaned.csv"
OUTPUT_CSV_WITH_COORDS = "IOS_Tenant_Targets_Wth_Coords_cleaned.csv"
OUTPUT_MAP_HTML = "index.html"
# ==========================


def fix_encoding(s: str):
    """
    Fix common mojibake and normalize punctuation for geocoding.
    Handles things like:
      - USÃ¢ÂÂ19  -> US-19
      - LeeÃ¢ÂÂs  -> Lee's
    """
    if not isinstance(s, str):
        return s

    # First attempt: latin1 -> utf8 roundtrip (your original approach)
    try:
        s2 = s.encode("latin1").decode("utf8")
        s = s2
    except Exception:
        pass

    # Then normalize common mojibake sequences that often survive the roundtrip
    replacements = {
        "Ã¢ÂÂ": "-",   # broken non-breaking hyphen
        "â": "-",     # another broken hyphen form
        "-": "-",       # real non-breaking hyphen (U+2011)
        "–": "-",       # en dash
        "—": "-",       # em dash
        "Ã¢ÂÂ": "'",   # broken apostrophe
        "â": "'",     # broken apostrophe form
        "’": "'",       # smart apostrophe
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)

    # Collapse weird whitespace
    s = " ".join(s.split())
    return s


def strip_suite(addr: str) -> str:
    """
    Remove suite/unit/building fragments that often break geocoding:
    ", Suite 708", ", Ste B", ", Unit A", ", Bldg 10", ", Building 202"
    Also removes trailing '#' fragments like "#101".
    Only used for geocoding; original Address is kept for display.
    """
    if not isinstance(addr, str):
        return addr

    # First fix encoding on the raw address, so our patterns see real characters
    addr = fix_encoding(addr)

    # Remove comma + Suite/Ste/Unit/Bldg/Building up to the next comma (or end)
    addr = re.sub(
        r",\s*(Suite|Ste\.?|Unit|Bldg\.?|Building)\b[^,]*",
        "",
        addr,
        flags=re.IGNORECASE,
    )

    # Remove inline "#" fragments (e.g. "#101")
    addr = re.sub(r"#\s*\w+", "", addr)

    # Collapse consecutive spaces
    addr = re.sub(r"\s{2,}", " ", addr).strip()

    # Strip any trailing commas/spaces
    addr = re.sub(r"[,\s]+$", "", addr)

    return addr


def load_and_clean(csv_path: str) -> pd.DataFrame:
    # Read with a forgiving encoding
    df = pd.read_csv(csv_path, encoding="latin1")

    # If the first row looks like headers (Tenant, Location, etc.), fix that
    first_row_values = [str(v).strip() for v in df.iloc[0].tolist()]
    if "Tenant" in first_row_values and "Location" in first_row_values:
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)

    # Required columns for the cleaned CSVs
    EXPECTED_COLS = ["Tenant", "Location", "Address", "City", "State"]
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Keep only required columns (order-independent)
    df = df[EXPECTED_COLS].copy()

    # Basic cleanup + encoding fix on key fields
    for col in EXPECTED_COLS:
        df[col] = df[col].astype(str).str.strip().apply(fix_encoding)

    # Build a CLEAN address string just for geocoding
    df["clean_address"] = df["Address"].apply(strip_suite)

    # full_address used for geocoding (append country to help Nominatim)
    df["full_address"] = (
        df["clean_address"] + ", " +
        df["City"] + ", " +
        df["State"] + ", USA"
    )

    return df


def geocode_addresses(
    df: pd.DataFrame,
    existing_cache: dict | None = None
) -> pd.DataFrame:
    """
    Geocode all addresses in df["full_address"].
    existing_cache: dict[full_address] -> (lat, lon)
    """
    geolocator = Nominatim(user_agent="ios_tenant_mapper", timeout=15)

    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=1,        # be nice to OSM
        max_retries=3,
        error_wait_seconds=5,
        swallow_exceptions=True     # don't raise, just return None
    )

    cache: dict[str, tuple[float | None, float | None]] = {}
    if existing_cache:
        cache.update(existing_cache)

    def geocode_one(addr: str):
        addr = (addr or "").strip()
        if not addr:
            return None, None

        if addr in cache:
            return cache[addr]

        try:
            loc = geocode(addr)
            if loc:
                result = (loc.latitude, loc.longitude)
            else:
                result = (None, None)
        except (GeocoderUnavailable, GeocoderTimedOut, ConnectionError) as e:
            print(f"Geocode error for '{addr}': {e}")
            result = (None, None)
        except Exception as e:
            print(f"Unexpected geocode error for '{addr}': {e}")
            result = (None, None)

        cache[addr] = result
        return result

    lats = []
    lons = []

    print("Geocoding addresses...")
    total = len(df)
    for i, addr in enumerate(df["full_address"], start=1):
        lat, lon = geocode_one(addr)
        lats.append(lat)
        lons.append(lon)

        # Only spam the console occasionally
        if i == 1 or i == total or i % 10 == 0:
            print(f"[{i}/{total}] {addr} -> ({lat}, {lon})")

    df["lat"] = lats
    df["lon"] = lons

    # Save failures separately for inspection
    df_failed = df[df["lat"].isna() | df["lon"].isna()].copy()
    before = len(df)
    df = df.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    after = len(df)

    print(f"Geocoding complete. Kept {after}/{before} rows with valid coordinates.")
    if not df_failed.empty:
        fail_path = "geocode_failures.csv"
        df_failed.to_csv(fail_path, index=False)
        print(f"{len(df_failed)} rows failed geocoding. Written to {os.path.abspath(fail_path)}")

    return df


def build_map(df: pd.DataFrame, output_html: str):
    # Center map on mean lat/lon
    center_lat = df["lat"].mean()
    center_lon = df["lon"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=5)

    # Simple color rotation for tenants
    color_cycle = [
        "red", "blue", "green", "purple", "orange",
        "darkred", "lightred", "beige", "darkblue",
        "darkgreen", "cadetblue", "darkpurple", "white",
        "pink", "lightblue", "lightgreen", "gray",
        "black", "lightgray"
    ]

    tenants = sorted(df["Tenant"].unique())
    color_map = {tenant: color_cycle[i % len(color_cycle)]
                 for i, tenant in enumerate(tenants)}

    # Create a layer for each tenant
    for tenant in tenants:
        tenant_df = df[df["Tenant"] == tenant]
        fg = folium.FeatureGroup(name=tenant, show=True)

        for _, row in tenant_df.iterrows():
            popup_lines = [
                f"<b>Tenant:</b> {row['Tenant']}",
                f"<b>Address:</b> {row['Address']}, {row['City']}, {row['State']}",
            ]

            popup_html = "<br>".join(popup_lines)

            folium.Marker(
                location=[row["lat"], row["lon"]],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=row["Tenant"],
                icon=folium.Icon(color=color_map[tenant])
            ).add_to(fg)

        fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    m.save(output_html)
    print(f"Map saved to: {os.path.abspath(output_html)}")


def main():
    # Always load the latest cleaned CSV
    df = load_and_clean(CSV_PATH)

    # Build a cache from any existing geocoded CSV to avoid re-hitting the API
    existing_cache = None
    coords_path = Path(OUTPUT_CSV_WITH_COORDS)
    if coords_path.exists():
        try:
            df_cached = pd.read_csv(coords_path, encoding="latin1")
            if {"full_address", "lat", "lon"}.issubset(df_cached.columns):
                existing_cache = {
                    str(row["full_address"]).strip(): (row["lat"], row["lon"])
                    for _, row in df_cached.dropna(subset=["lat", "lon"]).iterrows()
                }
                print(f"Loaded {len(existing_cache)} cached coordinates from {coords_path}.")
        except Exception as e:
            print(f"Could not load cache from {coords_path}: {e}")

    df = geocode_addresses(df, existing_cache=existing_cache)

    # Save a copy with coordinates for future reuse (avoid re-geocoding)
    df.to_csv(OUTPUT_CSV_WITH_COORDS, index=False)
    print(f"CSV with coordinates saved to: {os.path.abspath(OUTPUT_CSV_WITH_COORDS)}")

    build_map(df, OUTPUT_MAP_HTML)


if __name__ == "__main__":
    main()
