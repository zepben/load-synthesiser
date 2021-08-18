#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import argparse
import logging
import os
import shutil
from datetime import datetime, date
from pathlib import Path
from typing import Set, Dict, List

from zepben.evolve import connect, NetworkService, SyncNetworkConsumerClient, PowerTransformer, ConductingEquipment, \
    Feeder
import csv

from load_synthesiser.db.energy_profile import EnergyProfile, NetworkEnergyProfile
from load_synthesiser.db.id_date_range import IdDateRange
from load_synthesiser.db.sqlitedb import IndexSqliteDatabase, write_database, SqliteDatabase, SqliteReadingsDatabase

logger = logging.getLogger(__name__)


def load_energy_data(path: str, feeder_mrids, power_ratings: Dict[str, Dict[str, float]]) -> List[NetworkEnergyProfile]:
    """
    Example implementation that reads the load_profiles csv which contains load readings at the feeder head and allocates load to transformers in
    the feeder proportionally to their rating. Change this as you see fit.

    :param path: Path to the load_profiles csv file
    :param feeder_mrids: mRIDs of feeders to handle load data for
    :param power_ratings: Power ratings of transformers by their mRID.
    :return: A list of `NetworkEnergyProfile` containing a profile for each feeder, which contains a dictionary of dates to lists of EnergyProfiles,
    where each EnergyProfile represents the load profile of a single transformer. In this scenario this should be every transformer in the feeder.
    """
    neps = []
    energy_profile_by_date = dict()
    with open(path, "r") as f:
        reader = csv.reader(f)
        last_date = None
        kw = []
        feeder_mrid = None
        for row in reader:
            if row[0] not in feeder_mrids:
                continue

            if feeder_mrid is None:
                feeder_mrid = row[0]

            dt = datetime.fromisoformat(f"{row[1]} {row[2][:-1]}")
            if last_date is None:
                last_date = dt.date()

            if dt.date() != last_date:
                energy_profile_by_date[last_date] = []
                for mrid, rating in power_ratings[feeder_mrid].items():
                    # For each PowerTransformer on the feeder we apportion the load based on its rating
                    # There will be an EnergyProfile for each transformer for each day containing all the readings for that day.
                    ep = EnergyProfile(mrid, last_date.isoformat())
                    for w in kw:
                        ep.add_readings(w * rating, 0)
                    energy_profile_by_date[last_date].append(ep)
                last_date = dt.date()
                kw = []

            if row[3]:
                kw.append(float(row[3]))

            if row[0] != feeder_mrid:
                neps.append(NetworkEnergyProfile(feeder_mrid, set(power_ratings[feeder_mrid].keys()), energy_profile_by_date))
                energy_profile_by_date = dict()
                feeder_mrid = row[0]
        else:
            neps.append(NetworkEnergyProfile(feeder_mrid, set(power_ratings[feeder_mrid].keys()), energy_profile_by_date))
    return neps


def load_data(path: str, client: SyncNetworkConsumerClient, feeder_mrids: Set[str], output_dir, clean_dir):
    """
    Does some dumb load apportioning on some feeders and then writes out load databases to output_dir
    :param path: Path to the load CSV
    :param client: evolve client for talking to ewb gRPC service
    :param feeder_mrids: mRIDs of the desired feeders to process
    :param output_dir: Output directory for generated databases
    """
    # Create the output directory, throwing if it already exists.
    if os.path.exists(output_dir):
        if clean_dir:
            shutil.rmtree(os.path.join(os.getcwd(), output_dir))
        else:
            raise EnvironmentError(f"'{output_dir}' exists, please remove or provide --clean-data-dir parameter")
    Path(output_dir).mkdir(exist_ok=False)

    power_ratings = dict()

    # Fetch the relevant network and store it by feeder. This is used for
    for feeder_mrid in feeder_mrids:
        client.get_equipment_container(feeder_mrid, Feeder).throw_on_error()
        feeder = client.service.get(feeder_mrid, Feeder)

        total_power_rating = 0
        power_ratings[feeder_mrid] = dict()
        feeder_power_ratings = dict()
        # Apportion load between all transformers based on their rating
        for pt in filter(lambda x: isinstance(x, PowerTransformer), feeder.equipment):
            rated_s = pt.get_end_by_num(1).rated_s  # Only care about rated_s on HV side of the transformer
            if rated_s == 0:
                rated_s = pt.get_end_by_num(2).rated_s
            total_power_rating += rated_s
            feeder_power_ratings[pt.mrid] = rated_s

        for pt_mrid, r in feeder_power_ratings.items():
            power_ratings[feeder_mrid][pt_mrid] = r / total_power_rating

    # Parse input data and generate energy_profile_by_date.
    # Each date should have an EnergyProfile for every transformer with load data.
    # EnergyProfile is documented in energy_profile.py
    neps = load_energy_data(path, feeder_mrids, power_ratings)

    entity_ids = set()
    for nep in neps:
        entity_ids.update(nep.transformer_mrids)

    dbs: Dict[date, SqliteReadingsDatabase] = dict()
    for nep in neps:
        energy_profile_by_date: Dict[date, List[EnergyProfile]] = nep.energy_profile_by_date
        # Write or update the energy databases. There will be one for each date in EWBs required format
        for day, eps in energy_profile_by_date.items():
            db = dbs.get(day, write_database("data", day))
            dbs[day] = db
            db.write_entity_ids(entity_ids)  # Naively write all the entity ids as in this case we know every day will have the same set of IDs.
            db.write_readings(eps)

    for db in dbs.values():
        db.finalise()

    index_db = IndexSqliteDatabase("data/load-readings-index.sqlite")
    index_db.create_schema()
    index_db.write_entity_ids(entity_ids)
    # Write the index DB. Naive implementation that assumes all transformers in energy_profile_by_date contain data for every date.
    dates = sorted(neps[0].energy_profile_by_date.keys())
    for id in entity_ids:
        r = IdDateRange(id, dates[0], dates[-1])
        index_db.write_index(id, r)


def main():
    parser = argparse.ArgumentParser(description="Load synthesiser for power networks")
    parser.add_argument('feeder', help='Feeder mRID to load for this load data', nargs="+")
    parser.add_argument('--ewb-server', help='Host and port of grpc server', metavar="host", default="localhost")
    parser.add_argument('--load-csv', help='CSV file containing pq values', default="pq.csv")
    parser.add_argument('--rpc-port', help="The gRPC port for the server", default="50051")
    parser.add_argument('--conf-address', help="The address to retrieve auth configuration from", default="http://localhost/auth")
    parser.add_argument('--client-id', help='Auth0 M2M client id', default="")
    parser.add_argument('--client-secret', help='Auth0 M2M client secret', default="")
    parser.add_argument('--ca', help='CA trust chain', default="")
    parser.add_argument('--cert', help='Signed certificate for your client', default="")
    parser.add_argument('--key', help='Private key for signed cert', default="")
    parser.add_argument('--output-dir', help='Output directory for database files', default="data")
    parser.add_argument('--clean-data-dir', help='Clear the data directory if present', action="store_true")

    args = parser.parse_args()
    if not args.load_csv:
        print("No load data provided")
        return

    ca = cert = key = client_id = client_secret = None
    if not args.client_id or not args.client_secret or not args.ca or not args.cert or not args.key:
        logger.warning(
            f"Using an insecure connection as at least one of (--ca, --token, --cert, --key) was not provided.")
    else:
        with open(args.key, 'rb') as f:
            key = f.read()
        with open(args.ca, 'rb') as f:
            ca = f.read()
        with open(args.cert, 'rb') as f:
            cert = f.read()
        client_secret = args.client_secret
        client_id = args.client_id

    if not args.feeder:
        logger.error(f"At least one feeder must be provided.")

    with connect(host=args.ewb_server, rpc_port=args.rpc_port, conf_address=args.conf_address, client_id=client_id, client_secret=client_secret, pkey=key,
                 cert=cert, ca=ca) as channel:
        client = SyncNetworkConsumerClient(channel)

        load_data(args.load_csv, client, args.feeder, args.output_dir, args.clean_data_dir)


if __name__ == "__main__":
    main()
    # cProfile.run("main()")
