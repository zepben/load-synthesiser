#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from abc import ABC, abstractmethod
from typing import Generator, List
from bitstring import BitArray, BitStream
from dataclassy import dataclass


@dataclass
class Readings(object):

    @abstractmethod
    def num_channels(self):
        raise NotImplementedError()

    @abstractmethod
    def channel(self, i: int) -> List[float]:
        raise NotImplementedError()

    def get(self, i: int) -> float:
        if self.num_channels() == 1:
            return self.channel(1)[i]
        else:
            val = self.channel(1)[i]
            for idx in range(2, self.num_channels()):
                val += self.channel(idx)[i]
            return val

    @abstractmethod
    def __len__(self):
        raise NotImplementedError()

    def maximum(self):
        if self.length() <= 0:
            raise ValueError("Can't get a max of 0 readings")

        return max(self.values)

    def minimum(self):
        if self.length() <= 0:
            raise ValueError("Can't get a min of 0 readings")

        return min(self.values)

    @abstractmethod
    def copy(self):
        raise NotImplementedError()


class OneChannelReadings(Readings):
    _channel: List[float] = []

    def num_channels(self):
        return 1

    def __len__(self):
        return len(self._channel)

    def channel(self, i: int) -> List[float]:
        return self._channel

    def copy(self):
        return OneChannelReadings([])


class MultiChannelReadings(Readings):
    _channels: List[List[float]] = []

    def __init__(self):
        if not self.channels:
            raise ValueError("You must provide channels")

        if len(self.channels) == 1:
            raise ValueError("Use OneChannelReadings for one channel")

        chan_len = len(self.channels[0])
        for chan in self.channels:
            if len(chan) != chan_len:
                raise ValueError("All channels must have the same number of values")

    def num_channels(self):
        return len(self._channels)

    def __len__(self):
        return len(self._channels[0])

    def channel(self, i: int) -> List[float]:
        return self._channels[i - 1]

    def copy(self):
        return MultiChannelReadings([])


def sx(readings: Readings) -> BitStream:
    buffer: BitStream = BitStream()
    if readings.num_channels() > 127:
        raise ValueError("The maximum number of channels supported is 127")

    buffer.append(f'uint:8={readings.num_channels()}')
    buffer.append(f'uint:32={len(readings)}')

    for channel_num in range(1, readings.num_channels() + 1):
        channel = readings.channel(channel_num)
        to_add = [f'uint:8={channel_num}']
        all_zero = True
        for i in range(0, len(channel)):
            val = round(channel[i] * 1000)
            all_zero = all_zero & (val == 0)  # may not work?
            to_add.extend(encode_7bit_long(val))

        if all_zero:
            buffer.append(f'int:8={-channel_num}')
        else:
            for thing in to_add:
                buffer.append(thing)
    buffer.bytepos = 0
    return buffer


def encode_zigzag(val: int) -> int:
    return (val << 1) ^ (val >> 63)


def encode_7bit_long(val: int) -> List[str]:
    ret = []
    val = encode_zigzag(val)
    while True:
        lower_7_bits = val & 0x7f
        val = val >> 7
        if val != 0:
            lower_7_bits = lower_7_bits | 128

        ret.append(f'uint:8={lower_7_bits}')

        if val <= 0:
            break
    return ret
