import csv
import os
import sys
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Text, MetaData, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import DbConfig


class StagingData:
    def __init__(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()

        # Định nghĩa bảng
        self.logs_table, self.casualties_table, self.vehicles_table, self.datetime_table, \
        self.crash_table, self.description_table, self.location_table = self._define_tables()
        self.metadata.create_all(self.engine)

    def _define_tables(self):
        # Định nghĩa các bảng
        logs_table = Table('logs', self.metadata,
                           Column('id', Integer, primary_key=True, autoincrement=True),
                           Column('action', String(50)),
                           Column('details', Text),
                           Column('status', String(50)),
                           Column('timestamp', String(50))
                           )

        casualties_table = Table('casualties', self.metadata,
                                 Column('casualties_id', Integer, primary_key=True, autoincrement=True),
                                 Column('casualties', String(255)),
                                 Column('fatalities', Float),
                                 Column('serious_injuries', Float),
                                 Column('minor_injuries', Float)
                                 )

        vehicles_table = Table('vehicles', self.metadata,
                               Column('vehicles_id', Integer, primary_key=True, autoincrement=True),
                               Column('car_sedan', Float),
                               Column('bus', Float),
                               Column('bicycle', Float)
                               )

        datetime_table = Table('datetime', self.metadata,
                               Column('date_time_id', Integer, primary_key=True, autoincrement=True),
                               Column('year', Integer),
                               Column('month', Float),
                               Column('day_of_week', Float),
                               Column('hour', Float),
                               Column('approximate', Boolean)
                               )

        crash_table = Table('crash', self.metadata,
                            Column('crash_id', Integer, primary_key=True, autoincrement=True),
                            Column('lat_long', String(255)),
                            Column('date_time_id', Integer),
                            Column('description_id', Integer),
                            Column('vehicles_id', String(255)),
                            Column('casualties_id', String(255))
                            )

        description_table = Table('description', self.metadata,
                                  Column('description_id', Integer, primary_key=True, autoincrement=True),
                                  Column('severity', String(255)),
                                  Column('speed_limit', Integer),
                                  Column('midblock', Boolean, nullable=True),
                                  Column('intersection', Boolean, nullable=True),
                                  Column('road_position_horizontal', String(255)),
                                  Column('road_position_vertical', String(255)),
                                  Column('road_sealed', Boolean, nullable=True),
                                  Column('road_wet', Boolean, nullable=True),
                                  Column('weather', String(255)),
                                  Column('crash_type', String(255)),
                                  Column('lighting', String(255)),
                                  Column('traffic_controls', String(255)),
                                  Column('drugs_alcohol', String(255), nullable=True),
                                  Column('DCA_code', String(255), nullable=True),
                                  Column('comment', Text, nullable=True)
                                  )

        location_table = Table('location', self.metadata,
                               Column('lat_long', Integer, primary_key=True, autoincrement=True),
                               Column('latitude', Float),
                               Column('longitude', Float),
                               Column('country', String(255)),
                               Column('state', String(255)),
                               Column('local_government_area', String(255)),
                               Column('statistical_area', String(255), nullable=True),
                               Column('suburb', String(255))
                               )

        return logs_table, casualties_table, vehicles_table, datetime_table, crash_table, description_table, location_table

    def read_csv(self, file_path):
        if not os.path.exists(file_path):
            print(f"File {file_path} không tồn tại!")
            return []
        with open(file_path, mode='r', encoding='utf-8') as file:
            return list(csv.DictReader(file))

    def safe_float(self, value, default=0.0):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def safe_int(self, value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def write_log(self, conn, action, details, status):
        conn.execute(self.logs_table.insert().values(
            action=action,
            details=details,
            status=status,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))

    def process_casualties(self, conn, casualties):
        existing_record = conn.execute(
            self.casualties_table.select()
            .where(self.casualties_table.c.casualties == casualties['casualties'])
        ).mappings().fetchone()

        if existing_record:
            if (
                existing_record['fatalities'] != self.safe_float(casualties.get('fatalities')) or
                existing_record['serious_injuries'] != self.safe_float(casualties.get('serious_injuries')) or
                existing_record['minor_injuries'] != self.safe_float(casualties.get('minor_injuries'))
            ):
                conn.execute(
                    self.casualties_table.update()
                    .where(self.casualties_table.c.casualties_id == existing_record['casualties_id'])
                    .values(
                        fatalities=self.safe_float(casualties.get('fatalities')),
                        serious_injuries=self.safe_float(casualties.get('serious_injuries')),
                        minor_injuries=self.safe_float(casualties.get('minor_injuries'))
                    )
                )
                self.write_log(conn, "update", f"Updated casualties: {casualties['casualties']}", "success")
                return 'updated'
        else:
            conn.execute(
                self.casualties_table.insert().values(
                    casualties=casualties.get('casualties', ''),
                    fatalities=self.safe_float(casualties.get('fatalities')),
                    serious_injuries=self.safe_float(casualties.get('serious_injuries')),
                    minor_injuries=self.safe_float(casualties.get('minor_injuries'))
                )
            )
            self.write_log(conn, "insert", f"Inserted casualties: {casualties['casualties']}", "success")
            return 'inserted'

    def process_vehicles(self, conn, vehicle):
        existing_record = conn.execute(
            self.vehicles_table.select()
            .where(self.vehicles_table.c.vehicles_id == vehicle.get('vehicles_id'))
        ).mappings().fetchone()

        if existing_record:
            conn.execute(
                self.vehicles_table.update()
                .where(self.vehicles_table.c.vehicles_id == existing_record['vehicles_id'])
                .values(
                    car_sedan=self.safe_float(vehicle.get('car_sedan')),
                    bus=self.safe_float(vehicle.get('bus')),
                    bicycle=self.safe_float(vehicle.get('bicycle'))
                )
            )
            self.write_log(conn, "update", f"Updated vehicles: {vehicle['vehicles_id']}", "success")
            return 'updated'
        else:
            conn.execute(
                self.vehicles_table.insert().values(
                    vehicles_id=self.safe_int(vehicle.get('vehicles_id')),
                    car_sedan=self.safe_float(vehicle.get('car_sedan')),
                    bus=self.safe_float(vehicle.get('bus')),
                    bicycle=self.safe_float(vehicle.get('bicycle'))
                )
            )
            self.write_log(conn, "insert", f"Inserted vehicles: {vehicle['vehicles_id']}", "success")
            return 'inserted'

    def process_datetime(self, conn, datetime_record):
        existing_record = conn.execute(
            self.datetime_table.select()
            .where(self.datetime_table.c.date_time_id == datetime_record.get('date_time_id'))
        ).mappings().fetchone()

        if existing_record:
            conn.execute(
                self.datetime_table.update()
                .where(self.datetime_table.c.date_time_id == existing_record['date_time_id'])
                .values(
                    year=self.safe_int(datetime_record.get('year')),
                    month=self.safe_float(datetime_record.get('month')),
                    day_of_week=self.safe_float(datetime_record.get('day_of_week')),
                    hour=self.safe_float(datetime_record.get('hour')),
                    approximate=bool(datetime_record.get('approximate', False))
                )
            )
            self.write_log(conn, "update", f"Updated datetime: {datetime_record['date_time_id']}", "success")
            return 'updated'
        else:
            conn.execute(
                self.datetime_table.insert().values(
                    year=self.safe_int(datetime_record.get('year')),
                    month=self.safe_float(datetime_record.get('month')),
                    day_of_week=self.safe_float(datetime_record.get('day_of_week')),
                    hour=self.safe_float(datetime_record.get('hour')),
                    approximate=bool(datetime_record.get('approximate', False))
                )
            )
            self.write_log(conn, "insert", f"Inserted datetime: {datetime_record['date_time_id']}", "success")
            return 'inserted'

    def process_crash(self, conn, crash):
        existing_record = conn.execute(
            self.crash_table.select()
            .where(self.crash_table.c.crash_id == crash['crash_id'])
        ).mappings().fetchone()

        if existing_record:
            conn.execute(
                self.crash_table.update()
                .where(self.crash_table.c.crash_id == crash['crash_id'])
                .values(
                    lat_long=crash['lat_long'],
                    date_time_id=self.safe_int(crash.get('date_time_id')),
                    description_id=self.safe_int(crash.get('description_id')),
                    vehicles_id=crash['vehicles_id'],
                    casualties_id=crash['casualties_id']
                )
            )
            self.write_log(conn, "update", f"Updated crash: {crash['crash_id']}", "success")
            return 'updated'
        else:
            conn.execute(
                self.crash_table.insert().values(
                    lat_long=crash['lat_long'],
                    date_time_id=self.safe_int(crash.get('date_time_id')),
                    description_id=self.safe_int(crash.get('description_id')),
                    vehicles_id=crash['vehicles_id'],
                    casualties_id=crash['casualties_id']
                )
            )
            self.write_log(conn, "insert", f"Inserted crash: {crash['crash_id']}", "success")
            return 'inserted'

    def process_description(self, conn, description):
        existing_record = conn.execute(
            self.description_table.select()
            .where(self.description_table.c.description_id == description['description_id'])
        ).mappings().fetchone()

        if existing_record:
            conn.execute(
                self.description_table.update()
                .where(self.description_table.c.description_id == existing_record['description_id'])
                .values(
                    severity=description['severity'],
                    speed_limit=self.safe_int(description.get('speed_limit')),
                    midblock=self.safe_float(description.get('midblock')),
                    intersection=self.safe_float(description.get('intersection')),
                    road_position_horizontal=description['road_position_horizontal'],
                    road_position_vertical=description['road_position_vertical'],
                    road_sealed=self.safe_float(description.get('road_sealed')),
                    road_wet=self.safe_float(description.get('road_wet')),
                    weather=description['weather'],
                    crash_type=description['crash_type'],
                    lighting=description['lighting'],
                    traffic_controls=description['traffic_controls'],
                    drugs_alcohol=description['drugs_alcohol'],
                    DCA_code=description['DCA_code'],
                    comment=description['comment']
                )
            )
            self.write_log(conn, "update", f"Updated description: {description['description_id']}", "success")
            return 'updated'
        else:
            conn.execute(
                self.description_table.insert().values(
                    severity=description['severity'],
                    speed_limit=self.safe_int(description.get('speed_limit')),
                    midblock=self.safe_float(description.get('midblock')),
                    intersection=self.safe_float(description.get('intersection')),
                    road_position_horizontal=description['road_position_horizontal'],
                    road_position_vertical=description['road_position_vertical'],
                    road_sealed=self.safe_float(description.get('road_sealed')),
                    road_wet=self.safe_float(description.get('road_wet')),
                    weather=description['weather'],
                    crash_type=description['crash_type'],
                    lighting=description['lighting'],
                    traffic_controls=description['traffic_controls'],
                    drugs_alcohol=description['drugs_alcohol'],
                    DCA_code=description['DCA_code'],
                    comment=description['comment']
                )
            )
            self.write_log(conn, "insert", f"Inserted description: {description['description_id']}", "success")
            return 'inserted'

    def process_location(self, conn, location):
        existing_record = conn.execute(
            self.location_table.select()
            .where(self.location_table.c.lat_long == location['lat_long'])
        ).mappings().fetchone()

        if existing_record:
            conn.execute(
                self.location_table.update()
                .where(self.location_table.c.lat_long == location['lat_long'])
                .values(
                    latitude=self.safe_float(location.get('latitude')),
                    longitude=self.safe_float(location.get('longitude')),
                    country=location['country'],
                    state=location['state'],
                    local_government_area=location['local_government_area'],
                    statistical_area=location.get('statistical_area', ''),
                    suburb=location.get('suburb', '')
                )
            )
            self.write_log(conn, "update", f"Updated location: {location['lat_long']}", "success")
            return 'updated'
        else:
            conn.execute(
                self.location_table.insert().values(
                    latitude=self.safe_float(location.get('latitude')),
                    longitude=self.safe_float(location.get('longitude')),
                    country=location['country'],
                    state=location['state'],
                    local_government_area=location['local_government_area'],
                    statistical_area=location.get('statistical_area', ''),
                    suburb=location.get('suburb', '')
                )
            )
            self.write_log(conn, "insert", f"Inserted location: {location['lat_long']}", "success")
            return 'inserted'

    def staging_data(self):
        casualties_csv = os.path.join(self.db_config.data_dir, 'casualties.csv')
        vehicles_csv = os.path.join(self.db_config.data_dir, 'vehicles.csv')
        datetime_csv = os.path.join(self.db_config.data_dir, 'datetime.csv')
        crash_csv = os.path.join(self.db_config.data_dir, 'crash.csv')
        description_csv = os.path.join(self.db_config.data_dir, 'description.csv')
        location_csv = os.path.join(self.db_config.data_dir, 'location.csv')

        casualties_records = self.read_csv(casualties_csv)
        vehicles_records = self.read_csv(vehicles_csv)
        datetime_records = self.read_csv(datetime_csv)
        crash_records = self.read_csv(crash_csv)
        description_records = self.read_csv(description_csv)
        location_records = self.read_csv(location_csv)

        with self.engine.begin() as conn:
            inserted, updated, errors = 0, 0, 0

            for record in casualties_records:
                try:
                    result = self.process_casualties(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi casualties: {e}")

            for record in vehicles_records:
                try:
                    result = self.process_vehicles(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi vehicles: {e}")

            for record in datetime_records:
                try:
                    result = self.process_datetime(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi datetime: {e}")

            for record in crash_records:
                try:
                    result = self.process_crash(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi crash: {e}")

            for record in description_records:
                try:
                    result = self.process_description(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi description: {e}")

            for record in location_records:
                try:
                    result = self.process_location(conn, record)
                    if result == 'inserted':
                        inserted += 1
                    elif result == 'updated':
                        updated += 1
                except SQLAlchemyError as e:
                    errors += 1
                    print(f"Lỗi xử lý bản ghi location: {e}")

        # In kết quả tổng hợp
        print(f"Kết quả staging: Inserted: {inserted}, Updated: {updated}, Errors: {errors}")


if __name__ == "__main__":
    staging_data = StagingData()
    staging_data.staging_data()
    print("Dữ liệu đã được staging thành công vào cơ sở dữ liệu.")
