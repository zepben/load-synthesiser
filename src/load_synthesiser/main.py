#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import argparse
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Set, Dict, List

from zepben.evolve import connect, NetworkService, SyncNetworkConsumerClient, PowerTransformer, ConductingEquipment
import csv

from load_synthesiser.db.energy_profile import EnergyProfile, NetworkEnergyProfile
from load_synthesiser.db.id_date_range import IdDateRange
from load_synthesiser.db.sqlitedb import IndexSqliteDatabase, write_database

logger = logging.getLogger(__name__)


def load_energy_data(path: str, feeder_mrids, power_ratings: Dict[str, float]) -> NetworkEnergyProfile:
    """
    Example implementation that reads the load_profiles csv which contains load readings at the feeder head and allocates load to transformers in
    the feeder proportionally to their rating. Change this as you see fit.

    :param path: Path to the load_profiles csv file
    :param feeder_mrids: mRIDs of feeders to handle load data for
    :param power_ratings: Power ratings of transformers by their mRID.
    :return: A `NetworkEnergyProfile` containing a dictionary of dates to lists of EnergyProfiles, where each EnergyProfile represents the profile of a single
    transformer, and the set of all mRIDs that have an EnergyProfile. In this scenario this will be every transformer in each feeder queried.
    """
    energy_profile_by_date = dict()
    with open(path, "r") as f:
        reader = csv.reader(f)
        last_date = None
        kw = []
        for row in reader:
            if row[0] not in feeder_mrids:
                continue
            dt = datetime.fromisoformat(f"{row[1]} {row[2][:-1]}")
            if last_date is None:
                last_date = dt.date()
            if dt.date() != last_date:
                energy_profile_by_date[last_date] = []
                for mrid, rating in power_ratings.items():
                    # For each PowerTransformer on the feeder we apportion the load based on its rating
                    # There will be an EnergyProfile for each transformer for each day containing all the readings for that day.
                    ep = EnergyProfile(mrid, dt.date().isoformat())
                    for w in kw:
                        ep.add_readings(w * rating, 0)
                    energy_profile_by_date[last_date].append(ep)
                last_date = dt.date()
                kw = []
            if row[3]:
                kw.append(float(row[3]))
    return NetworkEnergyProfile(set(power_ratings.keys()), energy_profile_by_date)


def load_data(path: str, client: SyncNetworkConsumerClient, feeder_mrids: Set[str]):
    power_ratings = dict()

    # Fetch the relevant network and store it by feeder. This is used for
    for feeder_mrid in feeder_mrids:
        ns = NetworkService()
        client.get_feeder(ns, feeder_mrid).throw_on_error()

        total_power_rating = 0
        feeder_power_ratings = dict()
        # Apportion load between all transformers based on their rating
        for pt in ns.objects(PowerTransformer):
            rated_s = pt.get_end_by_num(1).rated_s  # Only care about rated_s on HV side of the transformer
            if rated_s == 0:
                rated_s = pt.get_end_by_num(2).rated_s
            total_power_rating += rated_s
            feeder_power_ratings[pt.mrid] = rated_s

        for pt_mrid, r in feeder_power_ratings.items():
            power_ratings[pt_mrid] = r / total_power_rating

    # Parse input data and generate energy_profile_by_date.
    # Each date should have an EnergyProfile for every transformer with load data.
    # EnergyProfile is documented in energy_profile.py
    nep = load_energy_data(path, feeder_mrids, power_ratings)
    energy_profile_by_date: Dict[date, List[EnergyProfile]] = nep.energy_profile_by_date

    # Write the energy databases. There will be one for each date in EWBs required format (data/<date
    for day, eps in energy_profile_by_date.items():
        Path(f"data/{day.isoformat()}").mkdir(parents=True)
        db = write_database(f"data/{day.isoformat()}/{day.isoformat()}-load-readings.sqlite")
        db.write_entity_ids(power_ratings.keys())

        db.write_readings(eps)
        db.finalise(day)

    # Write the index DB. Naive implementation that assumes all transformers in energy_profile_by_date contain data for every date.
    index_db = IndexSqliteDatabase("data/load-readings-index.sqlite")
    index_db.create_schema()
    index_db.write_entity_ids(nep.transformer_mrids)
    dates = sorted(energy_profile_by_date.keys())
    for id in nep.transformer_mrids:
        r = IdDateRange(id, dates[0], dates[1])
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

        load_data(args.load_csv, client, args.feeder)


if __name__ == "__main__":
    main()
    # cProfile.run("main()")
