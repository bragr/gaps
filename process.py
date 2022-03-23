import datetime
import enum
import gzip
import json
import logging
import math
import os

DATA_DIR = './data/samples.adsbexchange.com/readsb-hist/2022/03/01'
MILITARY = 0b0001  # military aircraft
INTERESTING = 0b0010  # ???
PIA = 0b0100  # Privacy ICAO aircraft address
LADD = 0b1000  # Limiting Aircraft Data Displayed
LAST_POS_THRESHOLD = 20.0
LAST_POS_DELTA = datetime.timedelta(seconds=LAST_POS_THRESHOLD)  # Consider coverage lost if we haven't gotten a position in this period
MEAN_EARTH_RADIUS_METERS = 6371008.7714  # https://en.wikipedia.org/wiki/Earth_radius#Published_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


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


class World(object):
    '''Representation of the world'''
    def __init__(self) -> None:
        self.aircraft = {}
    
    def process_aircraft(self, aircraft_update, update_time) -> None:
        hex = aircraft_update['hex']
        if aircraft_update.get('lastPosition') or aircraft_update.get('seen') >= LAST_POS_THRESHOLD:
            logging.debug(f"{hex} is a stale update")
            return
        
        aircraft = self.aircraft.get(hex, None)
        if aircraft:
            aircraft.process_update(aircraft_update, update_time)
        else:
            logging.debug(f"Adding aircraft {hex}: {aircraft_update}")
            self.aircraft[hex] = Aircraft(aircraft_update, update_time)


class Aircraft(object):
    '''Representation of a unique airframe/hexcode'''

    def __init__(self, update, update_time) -> None:
        self.hex = update['hex']
        self.registration = update.get('r', None)
        self.type = update.get('t', None)
        self._dbFlags = update.get('dbFlags', 0)
        self.current_flight = update.get('flight', self.registration if self.registration else self.hex)
        self.flights = {self.current_flight: Flight(update, update_time)}

    def process_update(self, update, update_time) -> None:
        if self.registration != update.get('r', self.registration):
            logging.debug(f"Found updated registration for {self}: ({self.registration}) -> ({update['r']})")
            self.registration = update.get('r', self.registration)

        if self.type != update.get('t', self.type):
            logging.debug(f"Found updated type for {self}: ({self.type}) -> ({update['t']})")
            self.type = update.get('t', self.type)

        if self.current_flight != update.get('flight', self.current_flight):
            logging.debug(f"Found updated flight for {self}: ({self.current_flight}) -> ({update.get('flight', self.current_flight)})")
            self.current_flight = update.get('flight', self.current_flight)
            self.flights[self.current_flight] = Flight(update, update_time)
            return

        self.flights[self.current_flight].process_update(update, update_time)

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


class Flight(object):
    '''Representation of a distinct flight of an aircraft e.g takeoff to landing'''

    def __init__(self, update, update_time) -> None:
        self.possitions = []
        self.process_update(update, update_time)

    def process_update(self, update, update_time) -> None:
        if not update.get('lat') or not update.get('lon'):
            logging.debug(f"hex {update['hex']}: Cowardly refusing to process updates with no position info")
            return

        if self.possitions:
            last_pos = self.possitions[-1]
            drop_time = update_time - last_pos.time
            if drop_time >= LAST_POS_DELTA:
                new_pos = Position(update, update_time)
                logging.warn(f"LOS/AOS hex: {update['hex']}:\n\tLOS: {last_pos}\n\tAOS: {new_pos}")
                logging.warn(f"\n\tGreat Circle: {great_circle(last_pos, new_pos)}m")
                self.possitions.append(new_pos)
                return

        self.possitions.append(Position(update, update_time))


class Position(object):
    '''Representation of a particular update'''
    def __init__(self, update, update_time) -> None:
        self.time = update_time
        self.lat = update['lat']
        self.long = update['lon']
        self.alt = update.get('alt_baro', update.get('alt_geom'))
        self.type = MessageType(update['type'])
    
    def __str__(self) -> str:
        return f"Latitude: {self.lat}, Longitude: {self.long}, Altitude: {self.alt}ft"


def great_circle(pos1, pos2) -> float:
    lat1 = math.radians(pos1.lat)
    long1 = math.radians(pos1.long)
    lat2 = math.radians(pos2.lat)
    long2 = math.radians(pos2.long)

    if long1 > long2:
        long_diff = long1 - long2
    else:
        long_diff = long2 - long1
    dist_radians = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(long_diff))
    return MEAN_EARTH_RADIUS_METERS * dist_radians


def main():
    world = World()
    files = os.listdir(DATA_DIR)
    files = sorted(files)

    for file in files:
        data_time = datetime.datetime.strptime(file, '%H%M%SZ.json.gz').replace(year=2022, month=3, day=1)
        logging.info(f"Loading data for time {data_time}")

        with gzip.open(f'{DATA_DIR}/{file}', 'r') as json_file:
            data = json.load(json_file)
        
        for aircraft in data['aircraft']:
            world.process_aircraft(aircraft, data_time)

if __name__ == "__main__":
    main()