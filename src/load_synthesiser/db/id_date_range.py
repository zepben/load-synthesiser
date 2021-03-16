#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from datetime import datetime, date

from bitstring import BitStream
from dataclassy import dataclass


@dataclass
class IdDateRange(object):
    id: str
    from_: date
    to: date

    def is_in_range(self, date: datetime):
        return self._from <= date <= self.to

    def sx(self):
        buffer = BitStream()
        buffer.append(f"uint:32={self.from_.year}")
        buffer.append(f"uint:8={self.from_.month}")
        buffer.append(f"uint:8={self.from_.day}")
        buffer.append(f"uint:32={self.to.year}")
        buffer.append(f"uint:8={self.to.month}")
        buffer.append(f"uint:8={self.to.day}")
        buffer.bytepos = 0
        return buffer
