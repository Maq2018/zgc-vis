import re
import csv
import ast
from shapely.geometry import MultiLineString
from shapely.wkt import loads, dumps
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

def to_wkt_multilinestring(coords):
    multi_line_string = MultiLineString(coords)
    wkt_string = dumps(multi_line_string)
    return wkt_string

def parse_wkt_linestring(wkt_string: str):
    line = loads(wkt_string)
    coordinates = list(line.coords)
    return coordinates

def literal_eval(s: str):
    return ast.literal_eval(s)
