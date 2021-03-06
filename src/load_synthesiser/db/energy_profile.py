#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from __future__ import annotations
from datetime import date
from typing import Callable, Set, Dict, List, Generator

from bitstring import BitStream
from dataclassy import dataclass

from load_synthesiser.db.readings import Readings, encode_7bit_long, OneChannelReadings


@dataclass
class NetworkEnergyProfile(object):
    """
    A EnergyProfile for a given Feeder
    """

    feeder_mrid: str
    "The mRID of the feeder"

    transformer_mrids: Set[str]
    "The mRIDs of the transformers on the feeder"

    energy_profile_by_date: Dict[date, List[EnergyProfile]]
    "A dictionary of dates to lists of EnergyProfiles, where each EnergyProfile represents the profile of a single transformer."


@dataclass
class EnergyProfile(object):
    """
    A profile of the kw in and kw out for a given mRID on a given date.
    There must always be the same number of kw_in as kw_out. Always use `add_readings` to add a new reading to ensure this.
    """

    id: str
    dt: date
    _kw_in: Readings = OneChannelReadings()
    _kw_out: Readings = OneChannelReadings()
    cacheable: bool = True

    def __init__(self):
        if len(self._kw_in) != len(self._kw_out):
            raise ValueError("Readings must have the same length")

    def add_readings(self, in_val: float, out_val: float = 0.0):
        """
        Add a reading to this profile
        :param in_val: The kw into this ID
        :param out_val: The kw out of this ID.
        :return:
        """
        self._kw_in.channel(1).append(in_val)
        self._kw_out.channel(1).append(out_val)

    @property
    def kw_in(self) -> Readings:
        return self._kw_in

    @property
    def kw_out(self) -> Readings:
        return self._kw_out

    def get_kw_in(self, i: int):
        return self._kw_in.get(i)

    def get_kw_out(self, i: int):
        return self._kw_out.get(i)

    def __len__(self):
        return len(self._kw_in)


@dataclass
class EnergyProfileStat(object):
    kw_in: float
    kw_out: float
    kw_net: float


def accumulating(energy_profile: EnergyProfile, accumulator: Callable[[float, float], float], finalise: Callable[[int, float], float], init_kw_in,
                 init_kw_out, init_kw_net):
    kwlen = len(energy_profile)
    if not kwlen:
        raise ValueError("Profile must have readings with a length")

    kw_in = init_kw_in
    kw_out = init_kw_out
    kw_net = init_kw_net
    for i in range(kwlen):
        kw_in = accumulator(kw_in, energy_profile.get_kw_in(i))
        kw_out = accumulator(kw_out, energy_profile.get_kw_in(i))
        kw_net = accumulator(kw_in, energy_profile.get_kw_in(i) - energy_profile.get_kw_out(i))
    return EnergyProfileStat(finalise(kwlen, kw_in), finalise(kwlen, kw_out), finalise(kwlen, kw_net))


def sx(stat: EnergyProfileStat):
    buffer = BitStream()

    to_add = encode_7bit_long(round(stat.kw_in * 1000))
    to_add.extend(encode_7bit_long(round(stat.kw_out * 1000)))
    to_add.extend(encode_7bit_long(round(stat.kw_net * 1000)))
    for thing in to_add:
        buffer.append(thing)
    buffer.bytepos = 0
    return buffer
