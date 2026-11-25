"""Extract Utah SNODAS SWE and snow-depth grid cells from one daily archive.

This script uses only the Python standard library. The input archive should be
the unmasked daily SNODAS tar from NSIDC, and the boundary zip should contain
the Census Bureau 1:500,000 state shapefile.
"""

from __future__ import annotations

import argparse
import array
import csv
import gzip
import io
import math
import re
import struct
import sys
import tarfile
import zipfile
from pathlib import Path


PRODUCTS = {
    "swe_mm": "11034",
    "snow_depth_mm": "11036",
}
GRID_KEYS = {
    "Number of columns": "columns",
    "Number of rows": "rows",
    "Benchmark x-axis coordinate": "benchmark_x",
    "Benchmark y-axis coordinate": "benchmark_y",
    "X-axis resolution": "x_resolution",
    "Y-axis resolution": "y_resolution",
    "No data value": "nodata",
    "Data bytes per pixel": "bytes_per_pixel",
}


def read_header(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result


def point_in_ring(lon: float, lat: float, ring: list[tuple[float, float]]) -> bool:
    """Return whether a point is inside a ring; boundary points count as inside."""
    inside = False
    previous_lon, previous_lat = ring[-1]
    for current_lon, current_lat in ring:
        cross = (lon - previous_lon) * (current_lat - previous_lat) - (
            lat - previous_lat
        ) * (current_lon - previous_lon)
        if abs(cross) < 1e-12 and min(previous_lon, current_lon) <= lon <= max(
            previous_lon, current_lon
        ) and min(previous_lat, current_lat) <= lat <= max(previous_lat, current_lat):
            return True
        if (current_lat > lat) != (previous_lat > lat):
            crossing_lon = (previous_lon - current_lon) * (lat - current_lat) / (
                previous_lat - current_lat
            ) + current_lon
            if lon < crossing_lon:
                inside = not inside
        previous_lon, previous_lat = current_lon, current_lat
    return inside


def read_dbf_state_record(dbf_data: bytes, state_field: str, state_value: str) -> int:
    if len(dbf_data) < 33:
        raise ValueError("State boundary DBF is too short")
    record_count = struct.unpack_from("<I", dbf_data, 4)[0]
    header_length, record_length = struct.unpack_from("<HH", dbf_data, 8)
    fields = []
    offset = 32
    while offset < header_length and dbf_data[offset] != 0x0D:
        descriptor = dbf_data[offset : offset + 32]
        name = descriptor[:11].split(b"\0", 1)[0].decode("ascii").strip()
        field_length = descriptor[16]
        fields.append((name, field_length))
        offset += 32

    field_names = [name for name, _ in fields]
    if state_field not in field_names:
        raise ValueError(f"Could not find {state_field} in state boundary DBF")
    field_index = field_names.index(state_field)
    field_offset = 1 + sum(length for _, length in fields[:field_index])
    field_length = fields[field_index][1]

    for index in range(record_count):
        start = header_length + index * record_length
        record = dbf_data[start : start + record_length]
        if record and record[0:1] != b"*":
            value = record[field_offset : field_offset + field_length].decode(
                "ascii", errors="ignore"
            ).strip()
            if value == state_value:
                return index
    raise ValueError(f"Boundary file has no record for {state_value}")


def read_shapefile_polygon(shp_data: bytes, record_index: int) -> list[list[tuple[float, float]]]:
    if len(shp_data) < 100:
        raise ValueError("State boundary SHP is too short")
    offset = 100
    index = 0
    while offset + 8 <= len(shp_data):
        content_length = struct.unpack_from(">I", shp_data, offset + 4)[0] * 2
        content_start = offset + 8
        content_end = content_start + content_length
        if content_end > len(shp_data):
            raise ValueError("State boundary SHP contains a truncated record")
        if index == record_index:
            shape_type = struct.unpack_from("<i", shp_data, content_start)[0]
            if shape_type not in (5, 15, 25):
                raise ValueError(f"Expected polygon geometry, found type {shape_type}")
            part_count, point_count = struct.unpack_from("<ii", shp_data, content_start + 36)
            parts_offset = content_start + 44
            point_offset = parts_offset + part_count * 4
            part_indices = list(struct.unpack_from(f"<{part_count}i", shp_data, parts_offset))
            part_indices.append(point_count)
            points = [
                struct.unpack_from("<dd", shp_data, point_offset + i * 16)
                for i in range(point_count)
            ]
            return [points[a:b] for a, b in zip(part_indices, part_indices[1:])]
        offset = content_end
        index += 1
    raise ValueError("State boundary SHP does not contain the selected DBF record")


def load_utah_polygon(boundary_zip: Path) -> list[list[tuple[float, float]]]:
    with zipfile.ZipFile(boundary_zip) as archive:
        shp_names = [name for name in archive.namelist() if name.lower().endswith(".shp")]
        dbf_names = [name for name in archive.namelist() if name.lower().endswith(".dbf")]
        if len(shp_names) != 1 or len(dbf_names) != 1:
            raise ValueError("Boundary zip must contain one state .shp and one .dbf")
        state_index = read_dbf_state_record(archive.read(dbf_names[0]), "STATEFP", "49")
        return read_shapefile_polygon(archive.read(shp_names[0]), state_index)


def load_product(tar: tarfile.TarFile, product_code: str) -> tuple[dict[str, str], bytes]:
    matching = [
        member
        for member in tar.getmembers()
        if product_code in Path(member.name).name and member.name.endswith(".txt.gz")
    ]
    if len(matching) != 1:
        raise ValueError(f"Expected one header for product {product_code}; found {len(matching)}")
    header_name = matching[0].name
    raster_name = header_name[:-7] + ".dat.gz"
    raster_member = tar.getmember(raster_name)
    header_text = gzip.decompress(tar.extractfile(matching[0]).read()).decode("ascii")
    raster_bytes = gzip.decompress(tar.extractfile(raster_member).read())
    return read_header(header_text), raster_bytes


def parse_grid(header: dict[str, str]) -> dict[str, float | int]:
    for key in ("Horizontal datum", "Geographically corrected", "Projected"):
        if key not in header:
            raise ValueError(f"SNODAS header is missing {key}")
    if header["Horizontal datum"].upper() != "WGS84" or header["Projected"].lower() != "no":
        raise ValueError("This extractor expects an unprojected WGS84 SNODAS grid")
    if header["Geographically corrected"].lower() != "yes":
        raise ValueError("SNODAS grid is not marked as geographically corrected")

    missing = [key for key in GRID_KEYS if key not in header]
    if missing:
        raise ValueError(f"SNODAS header is missing required fields: {', '.join(missing)}")
    grid: dict[str, float | int] = {}
    for header_key, output_key in GRID_KEYS.items():
        value = header[header_key]
        grid[output_key] = int(value) if output_key in ("columns", "rows", "bytes_per_pixel") else float(value)
    if grid["bytes_per_pixel"] != 2:
        raise ValueError("This extractor currently expects 2-byte SNODAS integer cells")
    return grid


def as_big_endian_int16(data: bytes, expected_values: int) -> array.array:
    values = array.array("h")
    values.frombytes(data)
    if len(values) != expected_values:
        raise ValueError(f"Raster has {len(values)} cells; header describes {expected_values}")
    if sys.byteorder == "little":
        values.byteswap()
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path("data/raw/SNODAS_unmasked_20261031.tar"),
        help="Path to one daily unmasked SNODAS tar archive",
    )
    parser.add_argument(
        "--boundary-zip",
        type=Path,
        default=Path("data/raw/cb_2025_us_state_500k.zip"),
        help="Census Bureau state boundary zip at 1:500,000 scale",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/snodas_utah_20261031.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    polygon = load_utah_polygon(args.boundary_zip)
    min_lon = min(lon for ring in polygon for lon, _ in ring)
    max_lon = max(lon for ring in polygon for lon, _ in ring)
    min_lat = min(lat for ring in polygon for _, lat in ring)
    max_lat = max(lat for ring in polygon for _, lat in ring)

    date_match = re.search(r"(\d{8})", args.archive.name)
    if not date_match:
        raise ValueError("Could not infer YYYYMMDD from the SNODAS archive filename")
    date_text = date_match.group(1)
    observation_date = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(args.archive, "r:") as archive:
        products = {}
        headers = {}
        grid = None
        for name, product_code in PRODUCTS.items():
            header, raster_data = load_product(archive, product_code)
            product_grid = parse_grid(header)
            if grid is None:
                grid = product_grid
            elif product_grid != grid:
                raise ValueError("SNODAS product headers do not describe the same grid")
            expected_values = int(grid["columns"]) * int(grid["rows"])
            products[name] = as_big_endian_int16(raster_data, expected_values)
            headers[name] = header

    assert grid is not None
    columns = int(grid["columns"])
    rows = int(grid["rows"])
    x0 = float(grid["benchmark_x"])
    y0 = float(grid["benchmark_y"])
    dx = float(grid["x_resolution"])
    dy = float(grid["y_resolution"])
    nodata = int(grid["nodata"])

    col_start = max(0, math.floor((min_lon - x0) / dx) - 1)
    col_stop = min(columns, math.ceil((max_lon - x0) / dx) + 2)
    row_start = max(0, math.floor((y0 - max_lat) / dy) - 1)
    row_stop = min(rows, math.ceil((y0 - min_lat) / dy) + 2)

    output_rows = 0
    valid_products = {name: 0 for name in PRODUCTS}
    no_data_counts = {name: 0 for name in PRODUCTS}
    with args.output.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "observation_date",
                "cell_id",
                "grid_row",
                "grid_col",
                "latitude",
                "longitude",
                "swe_mm",
                "snow_depth_mm",
            ]
        )
        for row in range(row_start, row_stop):
            latitude = y0 - row * dy
            for col in range(col_start, col_stop):
                longitude = x0 + col * dx
                if not any(point_in_ring(longitude, latitude, ring) for ring in polygon):
                    continue
                flat_index = row * columns + col
                values = {name: products[name][flat_index] for name in PRODUCTS}
                row_values = []
                for name in PRODUCTS:
                    value = values[name]
                    if value == nodata:
                        no_data_counts[name] += 1
                        row_values.append("")
                    else:
                        valid_products[name] += 1
                        row_values.append(value)
                writer.writerow(
                    [
                        observation_date,
                        f"snodas_r{row:04d}_c{col:04d}",
                        row,
                        col,
                        f"{latitude:.8f}",
                        f"{longitude:.8f}",
                        *row_values,
                    ]
                )
                output_rows += 1

    print(f"Observation date: {observation_date} 06:00 UTC")
    print(f"Utah boundary cells: {output_rows:,}")
    print(f"Grid window read: rows {row_start}:{row_stop}, columns {col_start}:{col_stop}")
    for name, header in headers.items():
        print(f"{name}: {header['Description']}; {header['Data units']}")
        print(f"  valid cells: {valid_products[name]:,}; NoData cells: {no_data_counts[name]:,}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
