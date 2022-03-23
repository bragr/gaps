import datetime
import enum
import gzip
import json
import logging
import math
import os
from tqdm import tqdm

DATA_DIR = './data/samples.adsbexchange.com/readsb-hist/2022/03/01'
MILITARY = 0b0001  # military aircraft
INTERESTING = 0b0010  # ???
PIA = 0b0100  # Privacy ICAO aircraft address
LADD = 0b1000  # Limiting Aircraft Data Displayed
LAST_POS_THRESHOLD = 60.0
LAST_POS_DELTA = datetime.timedelta(seconds=LAST_POS_THRESHOLD)  # Consider coverage lost if we haven't gotten a position in this period
MEAN_EARTH_RADIUS_METERS = 6371008.7714  # https://en.wikipedia.org/wiki/Earth_radius#Published_values

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')


class MessageType(enum.Enum):
    ADSB_ICAO = "adsb_icao"
    ADSB_ICAO_NT = "adsb_icao_nt"
    ADSR_ICAO = "adsr_icao"
    TISB_ICAO = "tisb_icao"
    ADSC = "adsc"
    MLAT = "mlat"
    OTHER = "other"
    MODE_S = "mode_s"
    ADSB_OTHER = "adsb_other"
    ADSR_OTHER = "adsr_other"
    TISB_OTHER = "tisb_other"
    TISB_TRACKFILE = "tisb_trackfile"


class Position(object):
    '''Representation of a particular update'''
    def __init__(self, update: dict, update_time: datetime.datetime) -> None:
        self.time = update_time
        self.lat = update['lat']
        self.long = update['lon']
        self.alt = update.get('alt_baro', update.get('alt_geom'))
        self.type = MessageType(update['type'])
    
    def __str__(self) -> str:
        return f"Latitude: {self.lat}, Longitude: {self.long}, Altitude: {self.alt}{'' if self.alt == 'ground' else 'ft'}"


class Dropout(object):
    '''Representation of a presumed coverage dropout of a particular flight'''
    def __init__(self, hex: str, start: Position, end: Position) -> None:
        self.hex = hex
        self.start = start
        self.end = end
    
    def asdict(self) -> dict:
        return {"lat1": self.start.lat, "long1": self.start.long, "alt1": self.start.alt,
                "lat2": self.end.lat, "long2": self.end.long, "alt2": self.end.alt}
    
    def great_circle(self) -> float:
        lat1 = math.radians(self.start.lat)
        long1 = math.radians(self.start.long)
        lat2 = math.radians(self.end.lat)
        long2 = math.radians(self.end.long)

        if long1 > long2:
            long_diff = long1 - long2
        else:
            long_diff = long2 - long1
        
        x = math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(long_diff)
        if x > 1.0:
            x = 1.0
        elif x < -1.0:
            x = -1.0
        dist_radians = math.acos(x)
        return MEAN_EARTH_RADIUS_METERS * dist_radians
    
    def __str__(self) -> str:
        return f"LOS: {self.start}\nAOS: {self.end}\nGreat Circle: {self.great_circle()}m"



class Flight(object):
    '''Representation of a distinct flight of an aircraft e.g takeoff to landing'''

    def __init__(self, update: dict, update_time: datetime.datetime) -> None:
        self.last_position = None
        self.process_update(update, update_time)

    def process_update(self, update: dict, update_time: datetime.datetime) -> Dropout:
        if not update.get('lat') or not update.get('lon'):
            return

        last_last_pos = self.last_position
        self.last_position = Position(update, update_time)
        if last_last_pos:
            update_delta = update_time - last_last_pos.time
            if update_delta >= LAST_POS_DELTA:
                dropout = Dropout(update['hex'], last_last_pos, self.last_position)
                return dropout
        return None


class Aircraft(object):
    '''Representation of a unique airframe/hexcode'''

    def __init__(self, update: dict, update_time: datetime.datetime) -> None:
        self.hex = update['hex']
        self.registration = update.get('r', None)
        self.type = update.get('t', None)
        self._dbFlags = update.get('dbFlags', 0)
        self.current_flight = update.get('flight', self.registration if self.registration else self.hex)
        self.flight = Flight(update, update_time)

    def process_update(self, update: dict, update_time: datetime.datetime) -> Dropout:
        if self.registration != update.get('r', self.registration):
            self.registration = update.get('r', self.registration)

        if self.type != update.get('t', self.type):
            self.type = update.get('t', self.type)

        if self.current_flight != update.get('flight', self.current_flight):
            self.current_flight = update.get('flight', self.current_flight)
            self.flight = Flight(update, update_time)
            return None

        return self.flight.process_update(update, update_time)

    @property
    def military(self) -> bool:
        return bool(self._dbFlags & MILITARY)

    @property
    def interesting(self) -> bool:
        return bool(self._dbFlags & INTERESTING)

    @property
    def pia(self) -> bool:
        return bool(self._dbFlags & PIA)

    @property
    def ladd(self) -> bool:
        return bool(self._dbFlags & LADD)

    def __str__(self) -> str:
        return f"Aircraft {self.registration}"


class World(object):
    '''Representation of the world'''
    def __init__(self) -> None:
        self.aircraft = {}
        self.dropouts = []
    
    def process_aircraft(self, aircraft_update: dict, update_time: datetime.datetime) -> None:
        hex = aircraft_update['hex']
        if aircraft_update.get('lastPosition') or aircraft_update.get('seen') >= LAST_POS_THRESHOLD:
            return
        
        aircraft = self.aircraft.get(hex, None)
        if aircraft:
            dropout = aircraft.process_update(aircraft_update, update_time)
            if dropout:
                self.dropouts.append(dropout)
        else:
            self.aircraft[hex] = Aircraft(aircraft_update, update_time)


def main() -> World:
    world = World()
    files = os.listdir(DATA_DIR)
    files = sorted(files)

    for file in tqdm(files):
        data_time = datetime.datetime.strptime(file, '%H%M%SZ.json.gz').replace(year=2022, month=3, day=1)
        logging.info(f"Loading data for time {data_time}")

        with gzip.open(f'{DATA_DIR}/{file}', 'r') as json_file:
            data = json.load(json_file)
        
        for aircraft in data['aircraft']:
            world.process_aircraft(aircraft, data_time)
    
    return world

if __name__ == "__main__":
    main()