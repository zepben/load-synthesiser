#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from bitstring import BitStream

__all__ = ["sx"]

true_value = BitStream()
false_value = BitStream()
true_value.append("uint:8=1")
false_value.append("uint:8=0")


def sx(item: bool) -> bytes:
    if item:
        true_value.bytepos = 0
        return true_value.read("bytes")
    else:
        true_value.bytepos = 0
        return false_value.read("bytes")
