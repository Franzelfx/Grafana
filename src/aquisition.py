import json
import time
import sqlite3
import requests
from sqlite3 import Error
from datetime import datetime
from loguru import logger

class AquisitionDBStore():
    def __init__(self, db_path: str = None):
        self.db_path = db_path
        self.dtu_data = None
        self.tasmota_data = None

    def get_dtu_data(self, dtu_url: str):
        try:
            url = f"{dtu_url}/api/record/live"
            response = requests.get(url)
            response.raise_for_status()
            response = response.json()
            self.dtu_data = response
        except requests.exceptions.HTTPError as e:
            print(e)
            logger.error(e)
        return response
    
    def get_tasmota_data(self, tasmota_url: str):
        """
        Get the data from the Tasmota energy meter
        """
        url = f"{tasmota_url}/cm?cmnd=Status%2010"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            data = data['StatusSNS']
            self.tasmota_data = data
            return data
        except requests.RequestException as e:
            print(f"Error fetching Tasmota data: {e}")
            return None
    
    @property
    def num_inverter(self):
        return len(self.dtu_data["inverter"])
    
    def num_cells(self, inverter: int):
        """
        Get the number of cells from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            num_cells: int
                The number of cells
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        p_dc_count = sum(1 for item in self.dtu_data["inverter"][inverter] if item["fld"] == "P_DC")
        return p_dc_count - 1
        

    def cell_power(self, inverter: int):
        """
        Get the cell powers from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_power: list
                The cell powers
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_power = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "P_DC":
                cell_power.append(item["val"])
        # Remove last element as it is the total power
        cell_power.pop()
        return cell_power

    def cell_voltage(self, inverter: int):
        """
        Get the cell voltages from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_voltage: list
                The cell voltages
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_voltage = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "U_DC":
                cell_voltage.append(item["val"])
        # Remove last element as it is the total voltage
        cell_voltage.pop()
        return cell_voltage
    
    def cell_current(self, inverter: int):
        """
        Get the cell currents from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_current: list
                The cell currents
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_current = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "I_DC":
                cell_current.append(item["val"])
        # Remove last element as it is the total current
        cell_current.pop()
        return cell_current

    def cell_yield_day(self, inverter: int):
        """
        Get the cell yield day from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_yield_day: float
                The cell yield day
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_yield_day = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "YieldDay":
                cell_yield_day.append(item["val"])
        # Remove last element as it is the total yield
        cell_yield_day.pop()
        return cell_yield_day

    def cell_yield_total(self, inverter: int):
        """
        Get the cell yield total from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_yield_total: float
                The cell yield total
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_yield_total = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "YieldTotal":
                cell_yield_total.append(item["val"])
        # Remove last element as it is the total yield
        cell_yield_total.pop()
        return cell_yield_total
    
    def cell_irradiation(self, inverter: int):
        """
        Get the cell irradiation from an inverter
        ----------------
        Parameters:
            inverter: int
                The inverter number
        Returns:
            cell_irradiation: float
                The cell irradiation
        """
        if inverter >= self.num_inverter:
            raise IndexError("Inverter index out of range")

        cell_irradiation = []
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "Irradiation":
                cell_irradiation.append(item["val"])
        return cell_irradiation
    
    def inverter_power(self, inverter: int):
        """
        Get the inverter power
        ----------------
        Returns:
            inverter_power: list
                The inverter power
        """
        # Inverter power is "P_AC"
        inverter_power: int
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "P_AC":
                inverter_power = item["val"]
        return inverter_power
    
    def inverter_yield_day(self, inverter: int):
        """
        Get the inverter yield day
        ----------------
        Returns:
            inverter_yield_day: list
                The inverter yield day
        """
        # Inverter yield day is "YieldDay"
        inverter_yield_day: int
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "YieldDay":
                inverter_yield_day = item["val"]
        return float(inverter_yield_day) / 1000 # Convert to kWh
    
    def inverter_yield_total(self, inverter: int):
        """
        Get the inverter yield total
        ----------------
        Returns:
            inverter_yield_total: list
                The inverter yield total
        """
        # Inverter yield total is "YieldTotal"
        inverter_yield_total: int
        for item in self.dtu_data["inverter"][inverter]:
            if item["fld"] == "YieldTotal":
                inverter_yield_total = item["val"]
        return float(inverter_yield_total) / 1000 # Convert to kWh

    def create_connection(self, db_file):
        """ Create a database connection to the SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            logger.error(e)
        return conn

    def create_table(self, conn):
        """ Create a table if it does not exist """
        try:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS inverter_data (
                              id INTEGER PRIMARY KEY,
                              timestamp REAL,
                              inverter_number REAL,
                              num_cells TEXT,
                              inverter_power TEXT,
                              yield_day TEXT,
                              yield_total TEXT)''')
        except Error as e:
            print(e)
            logger.error(e)

    def create_sum_table(self, conn):
        """ Create a sum table if it does not exist """
        try:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS inverter_sum_data (
                            id INTEGER PRIMARY KEY,
                            timestamp REAL,
                            power_sum REAL,
                            yield_day_sum REAL,
                            yield_total_sum REAL)''')
        except Error as e:
            print(e)
            logger.error(e)


    def insert_inverter_data(self, conn, data):
        """ Insert data into the table """
        sql = '''INSERT INTO inverter_data
                 (timestamp, inverter_number, num_cells, inverter_power, yield_day, yield_total)
                 VALUES (?, ?, ?, ?, ?, ?)'''
        try:
            cursor = conn.cursor()
            cursor.execute(sql, data)
        except Error as e:
            print(e)
            logger.error(e)

    def insert_sum_data(self, conn, data):
        """ Insert sum data into the table """
        sql = '''INSERT INTO inverter_sum_data (timestamp, power_sum, yield_day_sum, yield_total_sum)
                VALUES (?, ?, ?, ?)'''
        try:
            cursor = conn.cursor()
            cursor.execute(sql, data)
        except Error as e:
            print(e)
            logger.error(e)

    def create_energy_meter_table(self, conn):
        """ Create a table for the energy meter data if it does not exist """
        try:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS energy_meter_data (
                              id INTEGER PRIMARY KEY,
                              timestamp REAL,
                              total_out REAL,
                              total_in REAL,
                              power_in REAL,
                              meter_number TEXT)''')
        except Error as e:
            print(e)

    def insert_energy_meter_data(self, conn, data):
        """ Insert data into the energy meter table """
        sql = '''INSERT INTO energy_meter_data
                 (timestamp, total_out, total_in, power_in, meter_number)
                 VALUES (?, ?, ?, ?, ?)'''
        try:
            cursor = conn.cursor()
            cursor.execute(sql, data)
        except Error as e:
            print(e)

    def dump_to_db(self, db_path: str):
        # Connect to the SQLite database
        conn = self.create_connection(db_path)
        if conn is not None:
            # Create tables
            self.create_table(conn)
            self.create_sum_table(conn)
            self.create_energy_meter_table(conn)

            # Existing code to handle inverter data ...
            # Initialize variables for sums
            power_sum = 0
            yield_day_sum = 0
            yield_total_sum = 0

            # Get data from each inverter
            for inverter in range(self.num_inverter):
                # Get data
                timestamp = datetime.now().timestamp()
                inverter_power = self.inverter_power(inverter)
                yield_day = self.inverter_yield_day(inverter)
                yield_total = self.inverter_yield_total(inverter)
                data = (timestamp, inverter, self.num_cells(inverter), inverter_power, yield_day, yield_total)
                # Insert data
                self.insert_inverter_data(conn, data)

                # Add to sums
                power_sum += float(inverter_power)
                yield_day_sum += float(yield_day)
                yield_total_sum += float(yield_total)

            # Insert sum data
            sum_data = (timestamp, power_sum, yield_day_sum, yield_total_sum)
            self.insert_sum_data(conn, sum_data)

            # Get and insert energy meter data
            if self.tasmota_data:
                timestamp = datetime.now().timestamp()
                total_out = self.tasmota_data.get("", {}).get("Total_out", 0)
                total_in = self.tasmota_data.get("", {}).get("Total_in", 0)
                power_in = self.tasmota_data.get("", {}).get("Power_in", 0)
                meter_number = self.tasmota_data.get("", {}).get("Meter_Number", "")

                energy_meter_data = (timestamp, total_out, total_in, power_in, meter_number)
                self.insert_energy_meter_data(conn, energy_meter_data)

            # Commit the changes and close the connection
            conn.commit()
            conn.close()

            # Log Info
            logger.info("Data inserted into database")
            logger.info("-- Energy meter data --")
            logger.info(f"Timestamp: {timestamp}")
            logger.info(f"Power in: {power_in}")
            logger.info("-- Inverter data --")
            logger.info(f"Power sum: {power_sum}")
            logger.info(f"Yield day sum: {round(yield_day_sum, 2)}")
            logger.info(f"Yield total sum: {round(yield_total_sum, 2)}")
        else:
            print("Error! cannot create the database connection.")


# Usage
if __name__ == "__main__":
    # Load json config
    with open("./config.json") as f:
        config = json.load(f)
    
    # Configure logger to write to file
    logger.add(config["LOG_PATH"], rotation="1 day")
    while True:
        try:
            week = datetime.now().isocalendar()[1]
            home = AquisitionDBStore()
            home.get_dtu_data(config["DTU_URL"])
            home.get_tasmota_data(config["TASMOTA_URL"])
            home.dump_to_db(config["DB_PATH"])
            time.sleep(60)
        except Exception as e:
            print(e)
            logger.error(e)
            time.sleep(60)