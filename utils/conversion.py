import re
import csv
from shapely.wkt import loads
from io import StringIO
from typing import Union

KEEP_DIGITS = 6

def to_float(value: Union[str, float]) -> float:
    return round(float(value), KEEP_DIGITS)

# split string with comma, but ignore the comma inside the quote
def split_string(s: str):
    fp = StringIO(s)
    reader = csv.reader(fp, delimiter=',', quotechar='"')
    return next(reader)

def parse_wkt_multilinestring(wkt: str):
    pattern = r'\(([^()]+)\)'
    matches = re.findall(pattern, wkt)
    lines = []
    for match in matches:
        coords = [tuple(map(float, point.split())) for point in match.split(',')]
        lines.append(coords)
    return lines

def parse_wkt_linestring(wkt_string: str):
    line = loads(wkt_string)
    coordinates = list(line.coords)
    return coordinates
