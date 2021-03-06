"""
rtd.py

A WeeWX service to generate various near realtime data files.

Copyright (C) 2021 Gary Roderick                  gjroderick<at>gmail.com

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see https://www.gnu.org/licenses/.

Version: 0.1.0                                          Date: 4 October 2021

  Revision History
    4 October 2021      v0.1.0
        - initial release
"""
# python imports
import copy
import datetime
import errno
import json
import logging
import math
import os
import os.path
import socket
import sys
import threading
import time

from operator import itemgetter

# Python 2/3 compatibility shims
from six.moves import http_client
from six.moves import queue
from six.moves import urllib

# WeeWX imports
import user.gaugedata
import weewx
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil
import weewx.units
import weewx.wxformulas

from weewx.engine import StdService
from weewx.units import ValueTuple, convert, getStandardUnitType, ListOfDicts, as_value_tuple, _getUnitGroup
from weeutil.weeutil import get_object#, to_bool, to_int

# get a logger object
log = logging.getLogger(__name__)

# version number of this script
RTD_VERSION = '0.1.0'

# length of history to be maintained in seconds
MAX_AGE = 600

# list of obs that we will attempt to buffer
MANIFEST = ['outTemp', 'barometer', 'outHumidity', 'rain', 'rainRate',
            'humidex', 'windchill', 'heatindex', 'windSpeed', 'inTemp',
            'appTemp', 'dewpoint', 'windDir', 'UV', 'radiation', 'wind',
            'windGust', 'windGustDir', 'windrun']

# obs for which we need a history
HIST_MANIFEST = ['windSpeed', 'windDir', 'windGust', 'wind']


# ============================================================================
#                             class RealtimeData
# ============================================================================

class RealtimeData(StdService):
    """Service that generates various near realtime data files.

    The RealtimeData class creates and controls a number of threaded objects
    each of which are able to generate a different near realtime data file,
    eg: gauge-data.txt for the SteelSeries Weather Gauges,
        clientraw.txt for the Saratoga Weather Website templates,
        and others,


    dor that generates gauge-data.txt. Class
    RealtimeGaugeData feeds the RealtimeGaugeDataThread object with data via an
    instance of queue.Queue.
    """

    def __init__(self, engine, config_dict):
        # initialize my superclass
        super(RealtimeData, self).__init__(engine, config_dict)

        # first, log my version number
        log.info('version is %s' % RTD_VERSION)
        # create an empty list to hold information on the generators we are
        # using
        self.generators = []
        # get the RealtimeData config dictionary
        rtd_config_dict = config_dict.get('RealtimeData', {})
        # get a manager dict so we can access the database
        self.manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                       'wx_binding')
        self.db_manager = weewx.manager.open_manager(self.manager_dict)
        # obtain a Buffer object to buffer incoming loop packets
        # obtain the current time
        now = time.time()
        # get start of day and end of day timestamps so we can obtain a
        # Timespan object for the current day
        start_ts = weeutil.weeutil.startOfDay(now)
        end_dt = datetime.datetime.fromtimestamp(start_ts) + datetime.timedelta(days=1)
        end_ts = time.mktime(datetime.datetime.timetuple(end_dt))
        # obtain a Buffer object
        self.buffer = Buffer(MANIFEST,
                             weeutil.weeutil.TimeSpan(start_ts, end_ts))
        self.buffer['fred'] = 23
        log.info("self.buffer=%s" % (self.buffer,))
        # now get our generator objects
        for generator in rtd_config_dict.sections:
            self.generator_factory(generator,
                                   config_dict,
                                   rtd_config_dict[generator],
                                   engine)
        # did we end up with any generator objects?
        if len(self.generators) == 0:
            # we have no generator objects so we have nothing to do so return
            return
        else:
            # we do have some generator objects
            # The Buffer object needs to be seeded with some data. Best we can
            # do is obtain the today's day summary so far, it won't reflect
            # any loop packets that have arrived since the last archive record
            # but it is a good starting point.
            today_stats = self.db_manager._get_day_summary(now)
            # Did we get any stats? I could be we have an empty database.
            if today_stats:
                # we have stats, so set the Buffer object unit system and seed
                # the Buffer object
#                log.info('today_stats=%s' % (today_stats,))
                # TODO. Check that unit system is being properly set in buffer and cache under all circumstances
                self.buffer.std_unit_system = today_stats.get('unit_system')
                self.buffer.seed(today_stats)
            # now we can start the generators
            for thread in self.generators:
                thread['object'].start()
            # bind ourself to the relevant WeeWX events
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
            # self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
            # get a CachedPacket object as our loop packet cache
            self.packet_cache = CachedPacket()
            # if we have a non-None value for our Buffer unit system set our
            # packet cache to use the same unit system
            if self.buffer.std_unit_system is not None:
                self.packet_cache.std_unit_system = self.buffer.std_unit_system

    def generator_factory(self, generator, config_dict, gen_config_dict, engine):
        """Factory method to produce a generator thread object."""

        # obtain the generator class name
        # TODO. Can do away with the lookup table, maybe?
        generator_class = GENERATOR_LOOKUP.get(generator.lower())
        # did we find a generator class name?
        if generator_class is None:
            # an invalid generator block specified, log this and return
            log.info("Unknown generator '%s' specified. Ignoring." % generator)
            return
        # we have a valid generator class name so create queues for passing
        # data to and controlling the thread
        control_queue = queue.Queue()
        result_queue = queue.Queue()
        # now get the generator object
        generator_obj = get_object(generator_class)(control_queue=control_queue,
                                                    result_queue=result_queue,
                                                    config_dict=config_dict,
                                                    my_config_dict=gen_config_dict,
                                                    manager_dict=self.manager_dict,
                                                    engine=engine,
                                                    buffer=self.buffer,
                                                    lock=self.buffer.lock)
        # finally add our generator object and
        self.generators.append({'object': generator_obj,
                                'control_queue': control_queue,
                                'result_queue': result_queue})

    def new_loop_packet(self, event):
        """Puts new loop packets in the rtgd queue."""

        # update the buffer with the loop packet
#         log.info("pre-add buffer=%s" % (self.buffer,))
        self.buffer.add_packet(event.packet)
#         log.info("post-add buffer=%s" % (self.buffer,))
        # Our Buffer object unit system has now been set but what about our
        # packet cache? If not set then set it the same as the Buffer objects.
        if self.packet_cache.std_unit_system is None:
            self.packet_cache.std_unit_system = self.buffer.std_unit_system
        # update the packet cache
        self.packet_cache.update(event.packet, event.packet['dateTime'])
        # get a cached packet
        _cached_packet = self.packet_cache.get_packet(ts=event.packet['dateTime'])
        # package the loop packet in a dict since this is not the only data
        # we send via the queue
        _package = {'type': 'loop',
                    'payload': _cached_packet}
        # now put it in the queue for each generator
        for thread in self.generators:
            thread['control_queue'].put(_package)
        # do any logging that may be required
        if weewx.debug == 2:
            log.debug("queued cached loop packet (%s)" % _package['payload']['dateTime'])
        elif weewx.debug >= 3:
            log.debug("queued cached loop packet: %s" % _package['payload'])

    def shutDown(self):
        """Shut down any generators.

        Would normally do all of a given generators actions in one go but since
        we may have more than one thread and so that we don't have sequential
        (potential) waits of up to 15 seconds we send each thread a shutdown
        signal and then go and check that each has indeed shutdown.
        """

        # iterate over our generators and if the generator thread is alive send
        # it the shutdown signal
        for generator in self.generators:
            if generator['thread'].is_alive():
                # put a None in the control queue to signal to the thread to
                # shutdown
                generator['control_queue'].put(None)
        # now iterate over our generators again waiting for up to 15 seconds
        # for the generator thread to close. The generator threads should all
        # close at about the same time, so there should not be a wait of much
        # longer than 15 seconds unless they all failed to close!
        for generator in self.generators:
            # is the generator thread alive?
            if generator['thread'].is_alive():
                # wait up to 15 seconds for the thread to close
                generator['thread'].join(15.0)
                # log a short message about closure
                if generator['thread'].is_alive():
                    log.error("Unable to shut down '%s' thread" % generator['thread'].name)
                else:
                    log.debug("Shut down '%s' thread." % generator['thread'].name)

    def get_minmax_obs(self, obs_type):
        """Obtain the alltime max/min values for an observation."""

        # create an interpolation dict
        inter_dict = {'table_name': self.db_manager.table_name,
                      'obs_type': obs_type}
        # the query to be used
        minmax_sql = "SELECT MIN(min), MAX(max) FROM %(table_name)s_day_%(obs_type)s"
        # execute the query
        _row = self.db_manager.getSql(minmax_sql % inter_dict)
        if not _row or None in _row:
            return {'min_%s' % obs_type: None,
                    'max_%s' % obs_type: None}
        else:
            return {'min_%s' % obs_type: _row[0],
                    'max_%s' % obs_type: _row[1]}

    def get_rain(self, tspan):
        """Calculate rainfall over a given timespan."""

        _result = {}
        _rain_vt = self.db_manager.getAggregate(tspan, 'rain', 'sum')
        if _rain_vt:
            return _rain_vt
        else:
            return None


# ============================================================================
#                          class ObservationBuffer
# ============================================================================

class ObservationBuffer(object):
    """Base class to buffer an observation."""

    def __init__(self, stats, units=None, history=False):
        self.units = units
        self.last = None
        self.lasttime = None
        if history:
            self.use_history = True
            self.history_full = False
            self.history = []
        else:
            self.use_history = False

    def add_value(self, val, ts, hilo=True):
        """Add a value to my hilo and history stats as required."""

        pass

    def day_reset(self):
        """Reset the vector obs buffer."""

        pass

    def trim_history(self, ts):
        """Trim any old data from the history list."""

        # calc ts of oldest sample we want to retain
        oldest_ts = ts - MAX_AGE
        # set history_full property
        self.history_full = min([a.ts for a in self.history if a.ts is not None]) <= oldest_ts
        # remove any values older than oldest_ts
        self.history = [s for s in self.history if s.ts > oldest_ts]

    def history_max(self, ts, age=MAX_AGE):
        """Return the max value in my history.

        Search the last age seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp to start searching back from
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is a 3 way tuple of
            (value, x component, y component) and ts is the timestamp when
            it occurred.
        """

        born = ts - age
        snapshot = [a for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            _max = max(snapshot, key=itemgetter(1))
            return ObsTuple(_max[0], _max[1])
        else:
            return None

    def history_avg(self, ts, age=MAX_AGE):
        """Return the average value in my history.

        Search the last age seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp to start searching back from
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is a 3 way tuple of
            (value, x component, y component) and ts is the timestamp when
            it occurred.
        """

        born = ts - age
        snapshot = [a.value for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            return float(sum(snapshot)/len(snapshot))
        else:
            return None


# ============================================================================
#                             class VectorBuffer
# ============================================================================

class VectorBuffer(ObservationBuffer):
    """Class to buffer vector observations."""

    default_init = (None, None, None, None, None, 0.0, 0.0, 0.0, 0.0, 0)

    def __init__(self, stats, units=None, history=False):
        # initialize my superclass
        super(VectorBuffer, self).__init__(stats, units=units, history=history)

        if stats:
            self.min = stats.min
            self.mintime = stats.mintime
            self.max = stats.max
            self.max_dir = stats.max_dir
            self.maxtime = stats.maxtime
            self.sum = stats.sum
            self.xsum = stats.xsum
            self.ysum = stats.ysum
            self.sumtime = stats.sumtime
            self.count = stats.count
        else:
            (self.min, self.mintime,
             self.max, self.max_dir,
             self.maxtime, self.sum,
             self.xsum, self.ysum,
             self.sumtime, self.count) = VectorBuffer.default_init

    def add_value(self, val, ts, hilo=True):
        """Add a value to my hilo and history stats as required."""

        if val.mag is not None:
            if hilo:
                if self.min is None or val.mag < self.min:
                    self.min = val.mag
                    self.mintime = ts
                if self.max is None or val.mag > self.max:
                    self.max = val.mag
                    self.max_dir = val.dir
                    self.maxtime = ts
            self.sum += val.mag
            if self.lasttime:
                self.sumtime += ts - self.lasttime
            if val.dir is not None:
                self.xsum += val.mag * math.cos(math.radians(90.0 - val.dir))
                self.ysum += val.mag * math.sin(math.radians(90.0 - val.dir))
            if self.lasttime is None or ts >= self.lasttime:
                self.last = val
                self.lasttime = ts
            self.count += 1
            if self.use_history and val.dir is not None:
                self.history.append(ObsTuple(val, ts))
                self.trim_history(ts)

    def day_reset(self):
        """Reset the vector obs buffer."""

        (self.min, self.mintime,
         self.max, self.max_dir,
         self.maxtime, self.sum,
         self.xsum, self.ysum,
         self.sumtime, self.count) = VectorBuffer.default_init

    @property
    def day_vec_avg(self):
        """The day average vector."""

        try:
            _magnitude = math.sqrt((self.xsum**2 + self.ysum**2) / self.sumtime**2)
        except ZeroDivisionError:
            return VectorTuple(0.0, 0.0)
        _direction = 90.0 - math.degrees(math.atan2(self.ysum, self.xsum))
        _direction = _direction if _direction >= 0.0 else _direction + 360.0
        return VectorTuple(_magnitude, _direction)

    @property
    def history_vec_avg(self):
        """The history average vector.

        The period over which the average is calculated is the the history
        retention period (nominally 10 minutes).
        """

        # TODO. Check the maths here, time ?
        result = VectorTuple(None, None)
        if self.use_history and len(self.history) > 0:
            xy = [self.calc_xy(obs.value) for obs in self.history]
            xsum = sum(x for x, y in xy)
            ysum = sum(y for x, y in xy)
            oldest_ts = min(ob.ts for ob in self.history)
            _magnitude = math.sqrt((xsum**2 + ysum**2) / (time.time() - oldest_ts)**2)
            _direction = 90.0 - math.degrees(math.atan2(ysum, xsum))
            _direction = _direction if _direction >= 0.0 else _direction + 360.0
            result = VectorTuple(_magnitude, _direction)
        return result

    @property
    def history_vec_dir(self):
        """The history vector average direction.

        The period over which the average is calculated is the the history
        retention period (nominally 10 minutes).
        """

        result = None
        if self.use_history and len(self.history) > 0:
            xy = [self.calc_xy(obs.value) for obs in self.history]
            xsum = sum(x for x, y in xy)
            ysum = sum(y for x, y in xy)
            _direction = 90.0 - math.degrees(math.atan2(ysum, xsum))
            result = _direction if _direction >= 0.0 else _direction + 360.0
        return result

    @staticmethod
    def calc_xy(vector):
        """Given a vector observation value calculate the x and y components.

        Inputs:
            vector: a

        Returns a two way tuple in the format (x, y)
        """

        return (vector.mag * math.cos(math.radians(90.0 - vector.dir)),
                vector.mag * math.sin(math.radians(90.0 - vector.dir)))


# ============================================================================
#                             class ScalarBuffer
# ============================================================================

class ScalarBuffer(ObservationBuffer):
    """Class to buffer scalar observations."""

    default_init = (None, None, None, None, 0.0, 0)

    def __init__(self, stats, units=None, history=False):
        # initialize my superclass
        super(ScalarBuffer, self).__init__(stats, units=units, history=history)

        if stats:
            self.min = stats.min
            self.mintime = stats.mintime
            self.max = stats.max
            self.maxtime = stats.maxtime
            self.sum = stats.sum
            self.count = stats.count
        else:
            (self.min, self.mintime,
             self.max, self.maxtime,
             self.sum, self.count) = ScalarBuffer.default_init

    def add_value(self, val, ts, hilo=True):
        """Add a value to my stats as required."""

        if val is not None:
            if hilo:
                if self.min is None or val < self.min:
                    self.min = val
                    self.mintime = ts
                if self.max is None or val > self.max:
                    self.max = val
                    self.maxtime = ts
            self.sum += val
            if self.lasttime is None or ts >= self.lasttime:
                self.last = val
                self.lasttime = ts
            self.count += 1
            if self.use_history:
                self.history.append(ObsTuple(val, ts))
                self.trim_history(ts)

    def day_reset(self):
        """Reset the scalar obs buffer."""

        (self.min, self.mintime,
         self.max, self.maxtime,
         self.sum, self.count) = ScalarBuffer.default_init


# ============================================================================
#                               class Buffer
# ============================================================================

class Buffer(dict):
    """Class to buffer loop packet observations.

    # TODO. Needs rewording
    If archive based stats are an efficient means of getting stats for today.
    However, their use would mean that any daily stat (eg today's max outTemp)
    that 'occurs' after the most recent archive record but before the next
    archive record is written to archive will not be captured. For this reason
    selected loop data is buffered to ensure that such stats are correctly
    reflected.
    """

    def __init__(self, manifest, timespan):
        """Initialise an instance of our class."""

        # TODO. Do we need to call our parent?
        self.lock = threading.Lock()
        self.manifest = manifest
        self.timespan = timespan
        # timestamp of the last packet containing windSpeed, used for windrun
        # calculations
        self.last_windSpeed_ts = None
        self.std_unit_system = None

    def seed(self, stats):
        """Seed a buffer object with daily stats."""

        # Iterate over each observation type in the daily stats that is also in
        # our manifest. Obtain the seed function to use and call it.
        for obs_type in [f for f in stats if f in self.manifest]:
            # obtain the seed function
            seed_func = seed_functions.get(obs_type, Buffer.seed_scalar)
            # call it
            self.lock.acquire()
            seed_func(self, stats, obs_type, history=obs_type in HIST_MANIFEST)
            self.lock.release()

    def seed_scalar(self, stats, obs_type, history):
        """Seed a scalar buffer."""

        self[obs_type] = init_dict.get(obs_type, ScalarBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=history)

    def seed_vector(self, stats, obs_type, history):
        """Seed a vector buffer."""

        self[obs_type] = init_dict.get(obs_type, VectorBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=history)

    def add_packet(self, packet):
        """Add a packet to the buffer."""

        if packet['dateTime'] is not None:
            if not self.timespan.includesArchiveTime(packet['dateTime']):
                self.lock.acquire()
                self.start_of_day_reset()
                self.lock.release()
            if self.std_unit_system is None:
                self.lock.acquire()
                self.std_unit_system = packet['usUnits']
                self.lock.release()
            _conv_packet = weewx.units.to_std_system(packet, self.std_unit_system)
            for obs in [f for f in _conv_packet if f in self.manifest]:
                add_func = add_functions.get(obs, Buffer.add_value)
                self.lock.acquire()
                add_func(self, _conv_packet, obs)
                self.lock.release()

    def add_value(self, packet, obs):
        """Add a value to the buffer."""

        # if we haven't seen this obs before add it to our buffer
        if obs not in self:
            self[obs] = init_dict.get(obs, ScalarBuffer)(stats=None,
                                                         units=packet['usUnits'],
                                                         history=obs in HIST_MANIFEST)
        if self[obs].units == packet['usUnits']:
            _value = packet[obs]
        else:
            (unit, group) = getStandardUnitType(packet['usUnits'], obs)
            _vt = ValueTuple(packet[obs], unit, group)
            _value = weewx.units.convertStd(_vt, self[obs].units).value
        self[obs].add_value(_value, packet['dateTime'])

    def add_wind_value(self, packet, obs):
        """Add a wind value to the buffer."""

        # first add it as a scalar
        self.add_value(packet, obs)

        # if there is no windrun in the packet and if obs is windSpeed then we
        # can use windSpeed to update windrun
        if 'windrun' not in packet and obs == 'windSpeed':
            # has windrun been seen before, if not add it to the Buffer
            if 'windrun' not in self:
                self['windrun'] = init_dict.get(obs, ScalarBuffer)(stats=None,
                                                                   units=packet['usUnits'],
                                                                   history=obs in HIST_MANIFEST)
            # to calculate windrun we need a speed over a period of time, are
            # we able to calculate the length of the time period?
            if self.last_windSpeed_ts is not None:
                windrun = self.calc_windrun(packet)
                self['windrun'].add_value(windrun, packet['dateTime'])
            self.last_windSpeed_ts = packet['dateTime']

        # now add it as the special vector 'wind'
        if obs == 'windSpeed':
            if 'wind' not in self:
                self['wind'] = VectorBuffer(stats=None, units=packet['usUnits'])
            if self['wind'].units == packet['usUnits']:
                _value = packet['windSpeed']
            else:
                (unit, group) = getStandardUnitType(packet['usUnits'], 'windSpeed')
                _vt = ValueTuple(packet['windSpeed'], unit, group)
                _value = weewx.units.convertStd(_vt, self['wind'].units).value
            self['wind'].add_value(VectorTuple(_value, packet.get('windDir')),
                                   packet['dateTime'])

    def start_of_day_reset(self):
        """Reset our buffer stats at the end of an archive period.

        Reset our hi/lo data but don't touch the history, it might need to be
        kept longer than the end of the archive period.
        """

        for obs in self.manifest:
            self[obs].day_reset()

    def calc_windrun(self, packet):
        """Calculate windrun given windSpeed."""

        val = None
        if packet['usUnits'] == weewx.US:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts) / 3600.0
            unit = 'mile'
        elif packet['usUnits'] == weewx.METRIC:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts) / 3600.0
            unit = 'km'
        elif packet['usUnits'] == weewx.METRICWX:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts)
            unit = 'meter'
        if self['windrun'].units == packet['usUnits']:
            return val
        else:
            _vt = ValueTuple(val, unit, 'group_distance')
            return weewx.units.convertStd(_vt, self['windrun'].units).value


# ============================================================================
#                            Configuration dictionaries
# ============================================================================

init_dict = ListOfDicts({'wind': VectorBuffer})
add_functions = ListOfDicts({'windSpeed': Buffer.add_wind_value})
seed_functions = ListOfDicts({'wind': Buffer.seed_vector})


# ============================================================================
#                              class ObsTuple
# ============================================================================

# A observation during some period can be represented by the value of the
# observation and the time at which it was observed. This can be represented
# in a 2 way tuple called an obs tuple. An obs tuple is useful because its
# contents can be accessed using named attributes.
#
# Item   attribute   Meaning
#    0    value      The observed value eg 19.5
#    1    ts         The epoch timestamp that the value was observed
#                    eg 1488245400
#
# It is valid to have an observed value of None.
#
# It is also valid to have a ts of None (meaning there is no information about
# the time the was was observed.

class ObsTuple(tuple):

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    @property
    def value(self):
        return self[0]

    @property
    def ts(self):
        return self[1]


# ============================================================================
#                              class VectorTuple
# ============================================================================

# A vector value can be represented as a magnitude and direction. This can be
# represented in a 2 way tuple called an vector tuple. A vector tuple is useful
# because its contents can be accessed using named attributes.
#
# Item   attribute   Meaning
#    0    mag        The magnitude of the vector
#    1    dir        The direction of the vector in degrees
#
# mag and dir may be None

class VectorTuple(tuple):

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    @property
    def mag(self):
        return self[0]

    @property
    def dir(self):
        return self[1]


# ============================================================================
#                            Class CachedPacket
# ============================================================================

class CachedPacket(object):
    """Class to cache loop packets.

    The purpose of the cache is to ensure that necessary fields for the
    generation of gauge-data.txt are continuously available on systems whose
    station emits partial packets. The key requirement is that the field
    exists, the value (numerical or None) is handled by method calculate().
    Method calculate() could be refactored to deal with missing fields, but
    this would either result in the gauges dials oscillating when a loop packet
    is missing an essential field, or overly complex code in method calculate()
    if field caching was to occur.

    The cache consists of a dictionary of value, timestamp pairs where
    timestamp is the timestamp of the packet when obs was last seen and value
    is the value of the obs at that time. None values may be cached.

    A cached loop packet may be obtained by calling the get_packet() method.
    """

    # These fields must be available in every loop packet read from the
    # cache.
    OBS = ["cloudbase", "windDir", "windrun", "inHumidity", "outHumidity",
           "barometer", "radiation", "rain", "rainRate", "windSpeed",
           "appTemp", "dewpoint", "heatindex", "humidex", "inTemp",
           "outTemp", "windchill", "UV", "maxSolarRad"]
    # fields we ignore when caching a packet
    IGNORE = ['dateTime', 'usUnits']

    def __init__(self, rec=None):
        """Initialise our cache object.

        The cache needs to be initialised to include all of the fields required
        by method calculate(). We could initialise all field values to None
        (method calculate() will interpret the None values to be '0' in most
        cases). The result on the gauge display may be misleading. We can get
        ballpark values for all fields by priming them with values from the
        last archive record. As the archive may have many more fields than rtgd
        requires, only prime those fields that rtgd requires.

        This approach does have the drawback that in situations where the
        archive unit system is different to the loop packet unit system the
        entire loop packet will be converted each time the cache is updated.
        This is inefficient.
        """

        self.cache = dict()
        if rec is not None:
            # if we have a dateTime field in our record block use that otherwise
            # use the current system time
            _ts = rec['dateTime'] if 'dateTime' in rec else int(time.time() + 0.5)
            # add the packet to the cache
            self.update(rec, _ts)
        else:
            # set the cache unit system if known
            self.std_unit_system = None

    def update(self, packet, ts):
        """Update the cache from a loop packet.

        If the loop packet uses a different unit system to that of the cache
        then convert the loop packet before adding it to the cache. Update any
        previously seen cache fields and add any loop fields that have not been
        seen before.
        """

        # does our cache have a non-None unit system?
        if self.std_unit_system is None:
            # we have no unit system so adopt the unit system of the packet
            self.std_unit_system = packet['usUnits']
        # convert our packet to the cache's unit system
        _conv_packet = weewx.units.to_std_system(packet, self.std_unit_system)
        # iterate over the obs in the packet that we cache and update the cache
        # as required
        for obs in [x for x in _conv_packet if x not in CachedPacket.IGNORE]:
            # we only add non-None observations to the cache
            if _conv_packet[obs] is not None:
                # add the observation value and it's 'timestamp' to the cache
                self.cache[obs] = {'value': _conv_packet[obs], 'ts': ts}

    def get_value(self, obs, ts, max_age):
        """Get an obs value from the cache.

        Return a value for a given obs from the cache. If the value is older
        than max_age then None is returned.
        """

        if obs in self.cache and ts - self.cache[obs]['ts'] <= max_age:
            return self.cache[obs]['value']
        return None

    def get_packet(self, ts=None, max_age=600):
        """Get a loop packet from the cache.

        Resulting packet may contain None values.
        """

        if ts is None:
            ts = int(time.time() + 0.5)
        packet = {'dateTime': ts, 'usUnits': self.std_unit_system}
        for obs in self.cache:
            packet[obs] = self.get_value(obs, ts, max_age)
        return packet


# ============================================================================
#                            class HttpPostExport
# ============================================================================

class HttpPostExport(object):
    """Class to handle HTTP posting of gauge-data.txt.

    Once initialised data is posted by calling the objects export method and
    passing the data to be posted.
    """

    def __init__(self, rtgd_config_dict, *_):

        # first find our config
        if 'HttpPost' in rtgd_config_dict:
            post_config_dict = rtgd_config_dict.get('HttpPost', {})
        else:
            post_config_dict = rtgd_config_dict
        # get the remote server URL if it exists, if it doesn't set it to None
        self.remote_server_url = post_config_dict.get('remote_server_url', None)
        # timeout to be used for remote URL posts
        self.timeout = to_int(post_config_dict.get('timeout', 2))
        # response text from remote URL if post was successful
        self.response = post_config_dict.get('response_text', None)

    def export(self, data):
        """Post the data."""

        self.post_data(data)

    def post_data(self, data):
        """Post data to a remote URL via HTTP POST.

        This code is modelled on the WeeWX restFUL API, but rather then
        retrying a failed post the failure is logged and then ignored. If
        remote posts are not working then the user should set debug=1 and
        restart WeeWX to see what the log says.

        The data to be posted is sent as a JSON string.

        Inputs:
            data: dict to sent as JSON string
        """

        # get a Request object
        req = urllib.request.Request(self.remote_server_url)
        # set our content type to json
        req.add_header('Content-Type', 'application/json')
        # POST the data but wrap in a try..except so we can trap any errors
        try:
            response = self.post_request(req, json.dumps(data,
                                                         separators=(',', ':'),
                                                         sort_keys=True))
        except (urllib.error.URLError, socket.error,
                http_client.BadStatusLine, http_client.IncompleteRead) as e:
            # an exception was thrown, log it and continue
            log.debug("Failed to post data: %s" % e)
        else:
            if 200 <= response.code <= 299:
                # No exception thrown and we got a good response code, but did
                # we get self.response back in a return message? Check for
                # self.response, if its there then we can return. If it's
                # not there then log it and return.
                if self.response is not None:
                    if self.response in response:
                        # did get 'success' so log it and continue
                        if weewx.debug == 2:
                            log.debug("Successfully posted data")
                    else:
                        # it's possible the POST was successful if a response
                        # code of 200 was received if under python3, check
                        # response code and give it the benefit of the doubt
                        # but log it anyway
                        if response.code == 200:
                            log.debug("Data may have been posted successfully. "
                                      "Response message was not received but a valid response code was received.")
                        else:
                            log.debug("Failed to post data: Unexpected response")
                return
            # we received a bad response code, log it and continue
            log.debug("Failed to post data: Code %s" % response.code())

    def post_request(self, request, payload):
        """Post a Request object.

        Inputs:
            request: urllib2 Request object
            payload: the data to sent

        Returns:
            The urllib2.urlopen() response
        """

        # Under python 3 POST data should be bytes or an iterable of bytes and
        # not of type str. So attempt to convert the POST data to bytes, if it
        # already is of type bytes an error will be thrown under python 3, be
        # prepared to catch this error.
        try:
            payload_b = payload.encode('utf-8')
        except TypeError:
            payload_b = payload
        # do the POST
        _response = urllib.request.urlopen(request,
                                           data=payload_b,
                                           timeout=self.timeout)
        return _response


# ============================================================================
#                            class RsyncExport
# ============================================================================

class RsyncExport(object):
    """Class to handle rsync of gauge-data.txt.

    Once initialised data is rsynced by calling the objects export method and
    passing the data to be rsynced.
    """

    def __init__(self, rtgd_config_dict, rtgd_path_file):

        # first find our config
        if 'Rsync' in rtgd_config_dict:
            rsync_config_dict = rtgd_config_dict.get('Rsync', {})
        else:
            rsync_config_dict = rtgd_config_dict
        self.rtgd_path_file = rtgd_path_file
        self.rsync_server = rsync_config_dict.get('rsync_server')
        self.rsync_port = rsync_config_dict.get('rsync_port')
        self.rsync_user = rsync_config_dict.get('rsync_user')
        self.rsync_ssh_options = rsync_config_dict.get('rsync_ssh_options',
                                                       '-o ConnectTimeout=1')
        self.rsync_remote_rtgd_dir = rsync_config_dict.get('rsync_remote_rtgd_dir')
        self.rsync_dest_path_file = os.path.join(self.rsync_remote_rtgd_dir,
                                                 rsync_config_dict.get('rtgd_file_name',
                                                                       'gauge-data.txt'))
        self.rsync_compress = to_bool(rsync_config_dict.get('rsync_compress',
                                                            False))
        self.rsync_log_success = to_bool(rsync_config_dict.get('rsync_log_success',
                                                               False))
        self.rsync_timeout = rsync_config_dict.get('rsync_timeout')
        self.rsync_skip_if_older_than = to_int(rsync_config_dict.get('rsync_skip_if_older_than',
                                                                     4))

    def export(self, data):
        """Rsync the data."""

        packet_time = datetime.datetime.fromtimestamp(data['dateTime'])
        self.rsync_data(packet_time)

    def rsync_data(self, packet_time):
        """Perform the actual rsync."""

        # don't upload if more than rsync_skip_if_older_than seconds behind.
        if self.rsync_skip_if_older_than != 0:
            now = datetime.datetime.now()
            age = now - packet_time
            if age.total_seconds() > self.rsync_skip_if_older_than:
                log.info("skipping packet (%s) with age: %d" % (packet_time, age.total_seconds()))
                return
        rsync_upload = weeutil.rsyncupload.RsyncUpload(local_root=self.rtgd_path_file,
                                                       remote_root=self.rsync_dest_path_file,
                                                       server=self.rsync_server,
                                                       user=self.rsync_user,
                                                       port=self.rsync_port,
                                                       ssh_options=self.rsync_ssh_options,
                                                       compress=self.rsync_compress,
                                                       delete=False,
                                                       log_success=self.rsync_log_success,
                                                       timeout=self.rsync_timeout)
        try:
            rsync_upload.run()
        except IOError as e:
            (cl, unused_ob, unused_tr) = sys.exc_info()
            log.error("rtgd.rsync_data: Caught exception %s: %s" % (cl, e))


# ============================================================================
#                          class RealtimeDataThread
# ============================================================================

class RealtimeDataThread(threading.Thread):
    """Base class for Thread object that processes near realtime WeeWX data."""

    def __init__(self, control_queue, result_queue, manager_dict, buffer, buffer_lock):
        # initialize my superclass
        threading.Thread.__init__(self)

        # we will run as a daemon
        self.setDaemon(True)

        # save the queues we may use
        self.control_queue = control_queue
        self.result_queue = result_queue
        # save the manager dict for later db access
        self.manager_dict = manager_dict
        # save a Buffer object
        self.buffer = buffer
        # save the buffer object lock
        self.buffer_lock = buffer_lock

    def run(self):
        """Entry point for the RealtimeDataThread object.

        This method is called when the RealtimeDataThread child object is
        started using .start() and should be overridden in each child object.
        This method is used to generate the child objects output. In most cases
        it will consist of an infinite loop monitoring one or more queues and
        generating and exporting data.
        """

        pass

    @staticmethod
    def export_factory(config_dict, path_file):
        """Simple factory method to produce an object to export a file."""

        exporter = None
        # do we have a legacy remote_server_url setting or a HttpPost stanza
        if 'HttpPost' in config_dict or 'remote_server_url' in config_dict:
            exporter = 'user.rtd.HttpPostExport'
        elif 'Rsync' in config_dict:
            exporter = 'user.rtd.RsyncExport'
        if exporter is None:
            exporter_object = None
        else:
            # get the exporter object
            exporter_object = get_object(exporter)(config_dict, path_file)
        return exporter_object


# lookup dict of supported generator classes
GENERATOR_LOOKUP = {'gaugedata': 'user.gaugedata.GaugeDataThread',
                    'clientraw': 'user.gaugedata.GaugeDataThread'}
