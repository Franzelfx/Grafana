import requests
import json
import time
from typing import List
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class Cell:
    power: float = 0
    yield_day: float = 0
    yield_total: float = 0

@dataclass
class Inverter:
    cells: List[Cell]
    yield_day: float = 0
    yield_total: float = 0
    power_factor_ac: float = 0

@dataclass
class InverterGroupMeasurement:
    timestamp: str
    inverters: List[Inverter]

def fetch_data():
    url = "http://ahoy-dtu.4vmokcwrsuz7jhgl.myfritz.net/api/record/live"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def map_to_dataclass(data):
    timestamp = datetime.now().isoformat()
    inverters = []

    for inv_data in data["inverter"]:
        cells = []
        cell_data = {}
        inverter_data = {"yield_day": 0, "yield_total": 0, "power_factor_ac": 0}

        for entry in inv_data:
            fld = entry['fld']
            val = float(entry['val'])

            # Accumulate cell-specific data
            if fld == 'P_DC':
                cell_data['power'] = val
            elif fld == 'YieldDay':
                cell_data['yield_day'] = val
            elif fld == 'YieldTotal':
                cell_data['yield_total'] = val

            # Accumulate inverter-specific data from the last occurrences
            if fld == 'PF_AC':
                inverter_data['power_factor_ac'] = val
            elif fld == 'YieldDay':
                inverter_data['yield_day'] = val
            elif fld == 'YieldTotal':
                inverter_data['yield_total'] = val

        # Cell data is added for each cell
        if cell_data:
            cells.append(Cell(**cell_data))

        inverters.append(Inverter(cells=cells, **inverter_data))

    return InverterGroupMeasurement(timestamp, inverters)

def dump_data(data, file_name):
    with open(file_name, 'a') as file:
        json.dump(asdict(data), file, indent=4)
        file.write('\n')

def main():
    while True:
        data = fetch_data()
        mapped_data = map_to_dataclass(data)
        week_number = datetime.now().isocalendar()[1]
        year = datetime.now().year
        file_name = f"{year}_{week_number}.json"
        dump_data(mapped_data, file_name)
        time.sleep(30)

if __name__ == "__main__":
    main()
