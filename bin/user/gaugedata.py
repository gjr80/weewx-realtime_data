"""
rtgd.py

A WeeWX service to generate a loop based gauge-data.txt.

Copyright (C) 2017-2021 Gary Roderick             gjroderick<at>gmail.com

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see https://www.gnu.org/licenses/.

Version: 0.5.0                                          Date: 15 September 2021

  Revision History
    15 September 2021   v0.1.0
        - initial release


A WeeWX service to generate a loop based gauge-data.txt.

Used to update the SteelSeries Weather Gauges in near real time.

Inspired by crt.py v0.5 by Matthew Wall, a WeeWX service to emit loop data to
file in Cumulus realtime format. Refer http://wiki.sandaysoft.com/a/Realtime.txt

Use of HTTP POST to send gauge-data.txt content to a remote URL inspired by
work by Alec Bennett. Refer https://github.com/wrybread/weewx-realtime_gauge-data.

Abbreviated instructions for use:

1.  Install the SteelSeries Weather Gauges for WeeWX and confirm correct
operation of the gauges with WeeWX. Refer to
https://github.com/mcrossley/SteelSeries-Weather-Gauges/tree/master/weather_server/WeeWX

2.  Put this file in $BIN_ROOT/user.

3.  Add the following stanza to weewx.conf:

[RealtimeGaugeData]
    # Date format to be used in gauge-data.txt. Default is %Y.%m.%d %H:%M
    date_format = %Y.%m.%d %H:%M

    # Path to gauge-data.txt. Relative paths are relative to HTML_ROOT. If
    # empty default is HTML_ROOT. If setting omitted altogether default is
    # /var/tmp
    rtgd_path = /home/weewx/public_html

    # File name (only) of file produced by rtgd. Optional, default is
    # gauge-data.txt.
    rtgd_file_name = gauge-data.txt

    # Remote URL to which the gauge-data.txt data will be posted via HTTP POST.
    # Optional, omit to disable HTTP POST.
    # If remote_server_url is specified, do not specify an rsync server.
    remote_server_url = http://remote/address

    # timeout in seconds for remote URL posts. Optional, default is 2
    timeout = 1

    # Text returned from remote URL indicating success. Optional, default is no
    # response text.
    response_text = success

    # Remote host to which the gauge-data.txt data will be synced via rsync.
    # Optional, omit to disable rsync to remote host.
    # If rsync_server is specified, do not specify a remote_server_url.
    #
    # Note: The rsync feature will only work in WeeWX v.4 and above.  In earlier
    # versions, rsyncing of single files is not supported by WeeWX' rsync
    # help function.
    #
    # To use rsync, passwordless ssh using public/private key must be
    # configured for authentication from the user account that weewx runs under on
    # this computer to the user account on the remote machine with write access to
    # the destination directory (rsync_remote_rtgd_dir).
    #
    # If you run logwatch on your system, the following lines will show in the
    # weewx section when they are non-zero.  The first line includes any
    # reporting files rsynced (if that is configured).  The others report timeouts
    # and write errors.  Small numbers are expected here as timeouts are purposely
    # defaulted to 1 second.  If taking to long to send, it's better to skip it
    # and send the next (as in fresher) gauge-data.txt file.
    #
    #    rsync: files uploaded                          27206
    #    rsync: gauge-data: IO timeout-data                 7
    #    rsync: gauge-data: connection timeouts            11
    #    rsync: gauge-data: write errors                    1
    #
    #
    # Fill out the following fields:
    #   rsync_server             : The server to which gauge-data.txt will be copied.
    #   rsync_user               : The userid on rsync_server with write
    #                              permission to rsync_remote_rtgd_dir.
    #   rsync_remote_rtgd_dir    : The directory on rsync_server where
    #                              gauge-data.txt will be copied.
    #   rsync_compress           : True to compress the file before sending.
    #                              Default is False.
    #   rsync_log_success        : True to write success with timing messages to
    #                              the log (for debugging).  Default is False.
    #   rsync_ssh_options        : ssh options Default is '-o ConnectTimeout=1'
    #                              (When connecting, time out in 1 second.)
    #   rsync_timeout            : I/O timeout. Default is 1.  (When sending,
    #                              timeout in 1 second.)
    #   rsync_skip_if_older_than : Don't bother to rsync if greater than this
    #                              number of seconds.  Default is 4.  (Skip this
    #                              and move on to the next if this data is older
    #                              than 4 seconds.
    # Use either the post method or the rsync method, not both.
    #rsync_server = emerald.johnkline.com
    #rsync_user = root
    #rsync_remote_rtgd_dir = /home/weewx/gauge-data
    #rsync_compress = False
    #rsync_log_success = False
    #rsync_ssh_options = "-o ConnectTimeout=1"
    #rsync_timeout = 1
    #rsync_skip_if_older_than = 4

    # Minimum interval (seconds) between file generation. Ideally
    # gauge-data.txt would be generated on receipt of every loop packet (there
    # is no point in generating more frequently than this); however, in some
    # cases the user may wish to generate gauge-data.txt less frequently. The
    # min_interval option sets the minimum time between successive
    # gauge-data.txt generations. Generation will be skipped on arrival of a
    # loop packet if min_interval seconds have NOT elapsed since the last
    # generation. If min_interval is 0 or omitted generation will occur on
    # every loop packet (as will be the case if min_interval < station loop
    # period). Optional, default is 0.
    min_interval =

    # Number of compass points to include in WindRoseData, normally
    # 8 or 16. Optional, default 16.
    windrose_points = 16

    # Period over which to calculate WindRoseData in seconds. Optional, default
    # is 86400 (24 hours).
    windrose_period = 86400

    # Binding to use for appTemp data. Optional, default 'wx_binding'.
    apptemp_binding = wx_binding

    # The SteelSeries Weather Gauges displays the content of the gauge-data.txt
    # 'forecast' field in the scrolling text display. The RTGD service can
    # populate the 'forecast' field from a number of sources. The available 
    # sources are:
    #
    # 1. a user specified text
    # 2. the first line of a text file
    # 3. Weather Underground forecast from the Weather Underground API
    # 4. Darksky forecast from the Darksky API
    # 5. Zambretti forecast from the WeeWX forecast extension
    #
    # The block to be used is specified using the scroller_source config 
    # option. The scroller_source should be set to one of the following strings 
    # to use the indicated block:
    # 1. text - to use user specified text
    # 2. file - to user the first line of a text file
    # 3. Weather Underground - to use a Weather Underground forecast
    # 4. Darksky - to use a Darksky forecast
    # 5. Zambretti - to use a Zambretti forecast
    # 
    # The scroller_source config option is case insensitive. A corresponding
    # second level config section (ie [[ ]]) is required for the block to be 
    # used. Refer to step 4 below for details. If the scroller_source config 
    # option is omitted or left blank the 'forecast' filed will be blank and no 
    # scroller text will be displayed.
    scroller_source = text|file|WU|DS|Zambretti

    # Update windrun value each loop period or just on each archive period.
    # Optional, default is False.
    windrun_loop = false

    # Stations that provide partial packets are supported through a cache that
    # caches packet data. max_cache_age is the maximum age  in seconds for
    # which cached data is retained. Optional, default is 600 seconds.
    max_cache_age = 600

    # It is possible to ignore the sensor contact check result for the station
    # and always set the gauge-data.txt SensorContactLost field to 0 (sensor
    # contact not lost). This option should be used with care as it may mask a
    # legitimate sensor lost contact state. Optional, default is False.
    ignore_lost_contact = False

    [[StringFormats]]
        # String formats. Optional.
        degree_C = %.1f
        degree_F = %.1f
        degree_compass = %.0f
        hPa = %.1f
        inHg = %.2f
        inch = %.2f
        inch_per_hour = %.2f
        km_per_hour = %.1f
        km = %.1f
        mbar = %.1f
        meter = %.0f
        meter_per_second = %.1f
        mile_per_hour = %.1f
        mile = %.1f
        mm = %.1f
        mm_per_hour = %.1f
        percent = %.0f
        uv_index = %.1f
        watt_per_meter_squared = %.0f

    [[Groups]]
        # Groups. Optional. Note not all available WeeWX units are supported
        # for each group.
        group_altitude = foot        # Options are 'meter' or 'foot'
        group_pressure = hPa         # Options are 'inHg', 'mbar', or 'hPa'
        group_rain = mm              # Options are 'inch' or 'mm'
        group_speed = km_per_hour    # Options are 'mile_per_hour',
                                       'km_per_hour' or 'meter_per_second'
        group_temperature = degree_C # Options are 'degree_F' or 'degree_C'

4.  If the scroller_source config option has been set add a second level config
stanza for the specified block. Config stanzas for each of the supported 
sources are:

    -   user specified text:

        # Specify settings to be used for user specified text block
        [[Text]]
            # user specified text to populate the 'forecast' field
            text = enter text here

    -   first line of text file:

        # Specify settings to be used for first line of text file block
        [[File]]
            # Path and file name of file to use as block for the 'forecast' 
            # field. Must be a text file, first line only of file is read.
            file = path/to/file/file_name

            # Interval (in seconds) between between file reads. Default is 1800.
            interval = 1800

    -   Weather Underground forecast
    
        # Specify settings to be used for Weather Underground forecast block
        [[WU]]
            # WU API key to be used when calling the WU API
            api_key = xxxxxxxxxxxxxxxx

            # Interval (in seconds) between forecast downloads. Default
            # is 1800.
            interval = 1800

            # Minimum period (in seconds) between  API calls. This prevents
            # conditions where a misbehaving program could call the WU API
            # repeatedly thus violating the API usage conditions.
            # Default is 60.
            api_lockout_period = 60

            # Maximum number attempts to obtain an API response. Default is 3.
            max_tries = 3

            # Forecast type to be used. Must be one of the following:
            #   3day - 3 day forecast
            #   5day - 5 day forecast
            #   7day - 7 day forecast
            #   10day - 10 day forecast
            #   15day - 15 day forecast
            # A user's content licensing agreement with The Weather Company
            # will determine which forecasts are available for a given API
            # key. The 5 day forecast is commonly available as a free service
            # for PWS owners. Default is 5day.
            forecast_type = 3day|5day|7day|10day|15day

            # The location to be used for the forecast. Must be one of:
            #   geocode - uses latitude/longitude to source the forecast
            #   iataCode - uses and IATA code to source the forecast
            #   icaoCode - uses an ICAO code to source the forecast
            #   placeid - uses a Place ID to source the forecast
            #   postalKey - uses a post code to source the forecast. Only
            #               supported in US, UK, France, Germany and Italy.
            # The format used for each of the location settings is:
            #   geocode
            #   iataCode, <code>
            #   icaoCode, <code>
            #   placeid, <place ID>
            #   postalKey, <country code>, <postal code>
            # Where:
            #   <code> is the code concerned
            #   <place ID> is the place ID
            #   <country code> is the two letter country code (refer https://docs.google.com/document/d/13HTLgJDpsb39deFzk_YCQ5GoGoZCO_cRYzIxbwvgJLI/edit#heading=h.d5imu8qa7ywg)
            #   <postal code> is the postal code
            # The default is geocode, If gecode is used then the station
            # latitude and longitude are used.
            location = enter location

            # Units to be used in the forecast text. Must be one of the following:
            #   e - English units
            #   m - Metric units
            #   s - SI units
            #   h - Hybrid(UK) units
            # Refer to https://docs.google.com/document/d/13HTLgJDpsb39deFzk_YCQ5GoGoZCO_cRYzIxbwvgJLI/edit#heading=h.ek9jds3g3p9i
            # Default is m.
            units = e|m|s|h

            # Language to be used in the forecast text. Refer to
            # https://docs.google.com/document/d/13HTLgJDpsb39deFzk_YCQ5GoGoZCO_cRYzIxbwvgJLI/edit#heading=h.9ph8uehobq12
            # for available languages and the corresponding language code.
            # Default is en-GB
            language = language code

    -   Darksky forecast

        # Specify settings to be used for Darksky forecast block
        [[DS]]
            # Key used to access Darksky API. String. Mandatory.
            api_key = xxxxxxxxxxxxxxxx

            # Latitude to use for forecast. Decimal degrees, negative for 
            # southern hemisphere. Optional. Default is station latitude.
            latitude = yy.yyyyy

            # Longitude to use for forecast. Decimal degrees, negative for 
            # western hemisphere. Optional. Default is station longitude.
            longitude = zz.zzzz

            # Darksky forecast text to use. String either minutely, hourly or 
            # daily. Optional. Default is hourly. Refer Darksky API 
            # documentation at 
            # https://darksky.net/dev/docs#forecast-request
            block = minutely|hourly|daily

            # Language to use. String. Optional. Default is en (English).
            # Available language codes are listed in the Darksky API
            # documentation at https://darksky.net/dev/docs#forecast-request
            language = en

            # Units to use in forecast text. String either auto, us, si, ca or
            # uk2. Optional. Default is ca. Available units codes are
            # explained in the Darksky API documentation at
            # https://darksky.net/dev/docs#forecast-request
            units = auto|us|si|ca|uk2

            # Interval (in seconds) between forecast downloads. Optional. 
            # Default is 1800.
            interval = 1800

            # Maximum number attempts to obtain an API response. Optional. 
            # Default is 3.
            max_tries = 3

    -   Zambretti forecast

        # Specify settings to be used for Zambretti forecast block
        [[Zambretti]]
            # Interval (in seconds) between forecast updates. Optional. 
            # Default is 1800.
            # Note. In order to use the Zambretti forecast block the WeeWX
            # forecast extension must be installed and the Zambretti forecast
            # enabled. RTGD reads the current Zambretti forecast every interval 
            # seconds. The forecast extension controls how often the Zambretti 
            # forecast is updated.
            interval = 1800
        
            # Maximum number attempts to obtain the forecast. Optional. Default
            # is 3.
            max_tries = 3

            # Time to wait (in seconds) between attempts to read the forecast. 
            # Optional. Default is 3.
            retry_wait = 3

5.  Add the RealtimeGaugeData service to the list of report services under
[Engines] [[WxEngine]] in weewx.conf:

[Engines]
    [[WxEngine]]
        report_services = ..., user.rtgd.RealtimeGaugeData

6.  If you intend to save the realtime generated gauge-data.txt in the same
location as the ss skin generated gauge-data.txt then you must disable the
skin generated gauge-data.txt by commenting out the [[[data]]] entry and all
subordinate settings under [CheetahGenerator] [[ToDate]] in
$SKIN_ROOT/ss/skin.conf:

[CheetahGenerator]
    encoding = html_entities
    [[ToDate]]
        [[[index]]]
            template = index.html.tmpl
        # [[[data]]]
        #     template = gauge-data.txt.tmpl

7.  Edit $SKIN_ROOT/ss/scripts/gauges.js and change the realTimeURL_weewx
setting (circa line 68) to refer to the location of the realtime generated
gauge-data.txt. Change the realtimeInterval setting (circa line 37) to reflect
the update period of the realtime gauge-data.txt in seconds. This setting
controls the count down timer and update frequency of the SteelSeries Weather
Gauges.

8.  Delete the file $HTML_ROOT/ss/scripts/gauges.js.

9.  Stop/start WeeWX

10.  Confirm that gauge-data.txt is being generated regularly as per the period
and nth_loop settings under [RealtimeGaugeData] in weewx.conf.

11.  Confirm the SteelSeries Weather Gauges are being updated each time
gauge-data.txt is generated.

To do:
    - hourlyrainTH and ThourlyrainTH. Need to populate these fields, presently
      set to 0.0 and 00:00 respectively.
    - Lost contact with station sensors is implemented for Vantage and
      Simulator stations only. Need to extend current code to cater for the
      WeeWX supported stations. Current code assume that contact is there
      unless told otherwise.
    - consolidate wind lists into a single list.

Handy things/conditions noted from analysis of SteelSeries Weather Gauges:
    - wind direction is from 1 to 360, 0 is treated as calm ie no wind
    - trend periods are assumed to be one hour except for barometer which is
      taken as three hours
    - wspeed is 10 minute average wind speed (refer to wind speed gauge hover
      and gauges.js
"""

# python imports
import copy
import datetime
import errno
import json
import logging
import os
import os.path
import threading
import time

# Python 2/3 compatibility shims
from six.moves import queue

# WeeWX imports
import weewx
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil
import weewx.units
import weewx.wxformulas

from weewx.units import ValueTuple, convert, getStandardUnitType, ListOfDicts, as_value_tuple, _getUnitGroup
from weeutil.weeutil import to_bool, to_int

# get a logger object
log = logging.getLogger(__name__)

# version number of this script
RTGD_VERSION = '0.5.0'
# version number (format) of the generated gauge-data.txt
GAUGE_DATA_VERSION = '14'

# ordinal compass points supported
COMPASS_POINTS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                  'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW', 'N']

# default units to use
# Default to Metric with speed in 'km_per_hour' and rain in 'mm'.
# weewx.units.MetricUnits is close but we need to change the rain units (we
# could use MetricWX but then we would need to change the speed units!)
# start by making a deepcopy
_UNITS = copy.deepcopy(weewx.units.MetricUnits)
# now set the group_rain and group_rainrate units
_UNITS['group_rain'] = 'mm'
_UNITS['group_rainrate'] = 'mm_per_hour'
# now assign to our defaults
DEFAULT_UNITS = _UNITS

# map WeeWX unit names to unit names supported by the SteelSeries Weather
# Gauges
UNITS_WIND = {'mile_per_hour':      'mph',
              'meter_per_second':   'm/s',
              'km_per_hour':        'km/h'}
UNITS_TEMP = {'degree_C': 'C',
              'degree_F': 'F'}
UNITS_PRES = {'inHg': 'in',
              'mbar': 'mb',
              'hPa':  'hPa'}
UNITS_RAIN = {'inch': 'in',
              'mm':   'mm'}
UNITS_CLOUD = {'foot':  'ft',
               'meter': 'm'}
GROUP_DIST = {'mile_per_hour':      'mile',
              'meter_per_second':   'km',
              'km_per_hour':        'km'}

# list of obs that we will attempt to buffer
MANIFEST = ['outTemp', 'barometer', 'outHumidity', 'rain', 'rainRate',
            'humidex', 'windchill', 'heatindex', 'windSpeed', 'inTemp',
            'appTemp', 'dewpoint', 'windDir', 'UV', 'radiation', 'wind',
            'windGust', 'windGustDir', 'windrun']

# obs for which we need a history
HIST_MANIFEST = ['windSpeed', 'windDir', 'windGust', 'wind']

# length of history to be maintained in seconds
MAX_AGE = 600

# Define station lost contact checks for supported stations. Note that at
# present only Vantage and FOUSB stations lost contact reporting is supported.
STATION_LOST_CONTACT = {'Vantage': {'field': 'rxCheckPercent', 'value': 0},
                        'FineOffsetUSB': {'field': 'status', 'value': 0x40},
                        'Ultimeter': {'field': 'rxCheckPercent', 'value': 0},
                        'WMR100': {'field': 'rxCheckPercent', 'value': 0},
                        'WMR200': {'field': 'rxCheckPercent', 'value': 0},
                        'WMR9x8': {'field': 'rxCheckPercent', 'value': 0},
                        'WS23xx': {'field': 'rxCheckPercent', 'value': 0},
                        'WS28xx': {'field': 'rxCheckPercent', 'value': 0},
                        'TE923': {'field': 'rxCheckPercent', 'value': 0},
                        'WS1': {'field': 'rxCheckPercent', 'value': 0},
                        'CC3000': {'field': 'rxCheckPercent', 'value': 0}
                        }
# stations supporting lost contact reporting through their archive record
ARCHIVE_STATIONS = ['Vantage']
# stations supporting lost contact reporting through their loop packet
LOOP_STATIONS = ['FineOffsetUSB']

# default field map
DEFAULT_FIELD_MAP = {'temp': {
                         'source': 'outTemp',
                         'format': '%.1f'
                     },
                     'tempTL': {
                         'source': 'outTemp',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'tempTH': {
                         'source': 'outTemp',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TtempTL': {
                         'source': 'outTemp',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'TtempTH': {
                         'source': 'outTemp',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'temptrend': {
                         'source': 'outTemp',
                         'aggregate': 'trend',
                         'aggregate_period': '3600',
                         'grace_period': '300',
                         'format': '%.1f'
                     },
                     'inTemp': {
                         'source': 'inTemp',
                         'format': '%.1f'
                     },
                     'inTempTL': {
                         'source': 'inTemp',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'inTempTH': {
                         'source': 'inTemp',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TinTempTL': {
                         'source': 'inTemp',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'TinTempTH': {
                         'source': 'inTemp',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'hum': {
                         'source': 'outHumidity',
                         'format': '%.1f'
                     },
                     'humTL': {
                         'source': 'outHumidity',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'humTH': {
                         'source': 'outHumidity',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'ThumTL': {
                         'source': 'outHumidity',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'ThumTH': {
                         'source': 'outHumidity',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'dew': {
                         'source': 'dewpoint',
                         'format': '%.1f'
                     },
                     'dewpointTL': {
                         'source': 'dewpoint',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'dewpointTH': {
                         'source': 'dewpoint',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TdewpointTL': {
                         'source': 'dewpoint',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'TdewpointTH': {
                         'source': 'dewpoint',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'wchill': {
                         'source': 'windchill',
                         'format': '%.1f'
                     },
                     'wchillTL': {
                         'source': 'windchill',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TwchillTL': {
                         'source': 'windchill',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'heatindex': {
                         'source': 'heatindex',
                         'format': '%.1f'
                     },
                     'heatindexTH': {
                         'source': 'heatindex',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TheatindexTH': {
                         'source': 'heatindex',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'apptemp': {
                         'source': 'appTemp',
                         'format': '%.1f'
                     },
                     'apptempTL': {
                         'source': 'appTemp',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'apptempTH': {
                         'source': 'appTemp',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TapptempTL': {
                         'source': 'appTemp',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'TapptempTH': {
                         'source': 'appTemp',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'press': {
                         'source': 'barometer',
                         'format': '%.1f'
                     },
                     'pressTL': {
                         'source': 'barometer',
                         'aggregate': 'min',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'pressTH': {
                         'source': 'barometer',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TpressTL': {
                         'source': 'barometer',
                         'aggregate': 'mintime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'TpressTH': {
                         'source': 'barometer',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'presstrendval': {
                         'source': 'barometer',
                         'aggregate': 'trend',
                         'aggregate_period': '3600',
                         'grace_period': '300',
                         'format': '%.1f'
                     },
                     'rfall': {
                         'source': 'rain',
                         'aggregate': 'sum',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'rrate': {
                         'source': 'rainRate',
                         'format': '%.1f'
                     },
                     'rrateTM': {
                         'source': 'rainRate',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     # 'hourlyrainTH': {},
                     # 'ThourlyrainTH': {},
                     'wlatest': {
                         'source': 'windSpeed',
                         'format': '%.1f'
                     },
                     'windTM': {
                         'source': 'windSpeed',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'wgust': {
                         'source': 'windGust',
                         'format': '%.1f'
                     },
                     'wgustTM': {
                         'source': 'windGust',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'TwgustTM': {
                         'source': 'windGust',
                         'aggregate': 'maxtime',
                         'aggregate_period': 'day',
                         'format': '%H:%M'
                     },
                     'bearing': {
                         'source': 'windDir',
                         'format': '%.1f'
                     },
                     'avgbearing': {
                         'source': 'windDir',
                         'format': '%.1f'
                     },
                     'bearingTM': {
                         'source': 'wind',
                         'aggregate': 'max_dir',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'windrun': {
                         'source': 'windrun',
                         'aggregate': 'sum',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'UV': {
                         'source': 'UV',
                         'format': '%.1f'
                     },
                     'UVTH': {
                         'source': 'UV',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'SolarRad': {
                         'source': 'radiation',
                         'format': '%.1f'
                     },
                     'SolarRadTM': {
                         'source': 'radiation',
                         'aggregate': 'max',
                         'aggregate_period': 'day',
                         'format': '%.1f'
                     },
                     'CurrentSolarMax': {
                         'source': 'maxSolarRad',
                         'format': '%.1f'
                     },
                     'cloudbasevalue': {
                         'source': 'cloudbase',
                         'format': '%.1f'
                     }
                     }


# ============================================================================
#                     Exceptions that could get thrown
# ============================================================================

class MissingApiKey(IOError):
    """Raised when an API key cannot be found for an external service"""


# ============================================================================
#                       class RealtimeGaugeDataThread
# ============================================================================

class RealtimeGaugeDataThread(threading.Thread):
    """Thread that generates gauge-data.txt in near realtime."""

    def __init__(self, control_queue, result_queue, config_dict, manager_dict,
                 latitude, longitude, altitude, lock):
        # Initialize my superclass:
        threading.Thread.__init__(self)

        # setup a few thread things
        self.setName('RealtimeGaugeDataThread')
        self.setDaemon(True)

        self.control_queue = control_queue
        self.result_queue = result_queue
        self.config_dict = config_dict
        self.manager_dict = manager_dict
        self.lock = lock
        # get our RealtimeGaugeData config dictionary
        rtgd_config_dict = config_dict.get('RealtimeGaugeData', {})

        # setup file generation timing
        self.min_interval = rtgd_config_dict.get('min_interval', None)
        self.last_write = 0  # ts (actual) of last generation

        # get our file paths and names
        _path = rtgd_config_dict.get('rtgd_path', '/var/tmp')
        _html_root = os.path.join(config_dict['WEEWX_ROOT'],
                                  config_dict['StdReport'].get('HTML_ROOT', ''))

        self.rtgd_path = os.path.join(_html_root, _path)
        self.rtgd_path_file = os.path.join(self.rtgd_path,
                                           rtgd_config_dict.get('rtgd_file_name',
                                                                'gauge-data.txt'))
        self.rtgd_path_file_tmp = self.rtgd_path_file + '.tmp'

        # get windrose settings
        try:
            self.wr_period = int(rtgd_config_dict.get('windrose_period',
                                                      86400))
        except ValueError:
            self.wr_period = 86400
        try:
            self.wr_points = int(rtgd_config_dict.get('windrose_points', 16))
        except ValueError:
            self.wr_points = 16

        # get our groups and format strings
        self.date_format = rtgd_config_dict.get('date_format',
                                                '%Y-%m-%d %H:%M')
        self.time_format = '%H:%M'
        self.temp_group = rtgd_config_dict['Groups'].get('group_temperature',
                                                         'degree_C')
        self.pres_group = rtgd_config_dict['Groups'].get('group_pressure',
                                                         'hPa')
        self.pres_format = rtgd_config_dict['StringFormats'].get(self.pres_group,
                                                                 '%.1f')
        self.wind_group = rtgd_config_dict['Groups'].get('group_speed',
                                                         'km_per_hour')
        # Since the SteelSeries Weather Gauges derives distance units from wind
        # speed units we cannot use knots because WeeWX does not know how to
        # use distance in nautical miles. If we have been told to use knot then
        # default to mile_per_hour.
        if self.wind_group == 'knot':
            self.wind_group = 'mile_per_hour'
        self.wind_format = rtgd_config_dict['StringFormats'].get(self.wind_group,
                                                                 '%.1f')
        self.rain_group = rtgd_config_dict['Groups'].get('group_rain',
                                                         'mm')
        # SteelSeries Weather Gauges don't understand cm so default to mm if we
        # have been told to use cm
        if self.rain_group == 'cm':
            self.rain_group = 'mm'
        self.rain_format = rtgd_config_dict['StringFormats'].get(self.rain_group,
                                                                 '%.1f')
        self.dir_group = 'degree_compass'
        self.dir_format = rtgd_config_dict['StringFormats'].get(self.dir_group,
                                                                '%.1f')
        self.rad_group = 'watt_per_meter_squared'
        self.rad_format = rtgd_config_dict['StringFormats'].get(self.rad_group,
                                                                '%.0f')
        # SteelSeries Weather gauges derives windrun units from wind speed
        # units, so must we
        self.dist_group = GROUP_DIST[self.wind_group]
        self.dist_format = rtgd_config_dict['StringFormats'].get(self.dist_group,
                                                                 '%.1f')
        self.alt_group = rtgd_config_dict['Groups'].get('group_altitude',
                                                        'meter')
        self.flag_format = '%.0f'

        # set up output units dict
        # first get the Groups config from our config dict
        _config_units_dict = rtgd_config_dict.get('Groups', {})
        # group_rainrate needs special attention; it needs to match group_rain.
        # If group_rain does not exist omit group_rainrate as it will be
        # picked up from the defaults.
        if 'group_rain' in _config_units_dict:
            _config_units_dict['group_rainrate'] = "%s_per_hour" % (_config_units_dict['group_rain'],)
        # add the Groups config to the chainmap and set the units_dict property
        self.units_dict = ListOfDicts(_config_units_dict, DEFAULT_UNITS)
        # setup the field map
        _field_map = rtgd_config_dict.get('FieldMap', DEFAULT_FIELD_MAP)
        # update the field map with any extensions
        _extensions = rtgd_config_dict.get('FieldMapExtensions', {})
        # update the field map with any extensions
        _field_map.update(_extensions)

        # convert any defaults to ValueTuples
        for field in list(_field_map.items()):
            field_config = field[1]
            _group = _getUnitGroup(field_config['source'])
            _default = field_config.get('default', None)
            if _default is None:
                # no default specified so use 0 in output units
                _vt = ValueTuple(0, self.units_dict[_group], _group)
            elif len(_default) == 1:
                # just a value so use it in output units
                _vt = ValueTuple(float(_default), self.units_dict[_group], _group)
            elif len(_default) == 2:
                # we have a value and units so use that value in those units
                _vt = ValueTuple(float(_default[0]), self.units_dict[_group], _default[1])
            elif len(_default) == 3:
                # we already have a ValueTuple so nothing to do
                _vt = _default
            _field_map[field[0]]['default'] = _vt
        self.field_map = _field_map

        # get max cache age
        self.max_cache_age = rtgd_config_dict.get('max_cache_age', 600)

        # initialise last wind directions for use when respective direction is
        # None. We need latest and average
        self.last_dir = 0
        self.last_average_dir = 0

        # Are we updating windrun using archive data only or archive and loop
        # data?
        self.windrun_loop = to_bool(rtgd_config_dict.get('windrun_loop',
                                                         False))

        # Lost contact
        # do we ignore the lost contact 'calculation'
        self.ignore_lost_contact = to_bool(rtgd_config_dict.get('ignore_lost_contact',
                                                                False))
        # set the lost contact flag, assume we start off with contact
        self.lost_contact_flag = False

        # initialise the packet unit dict
        self.packet_unit_dict = None

        # initialise some properties used to hold archive period wind data
        self.windSpeedAvg_vt = ValueTuple(None, 'km_per_hour', 'group_speed')
        self.min_barometer = None
        self.max_barometer = None

        self.db_manager = None
        self.day_span = None

        self.packet_cache = None

        self.buffer = None
        self.rose = None
        self.last_rain_ts = None

        # initialise the scroller text
        self.scroller_text = None

        # get some station info
        self.latitude = latitude
        self.longitude = longitude
        self.altitude_m = altitude
        self.station_type = config_dict['Station']['station_type']

        # gauge-data.txt version
        self.version = str(GAUGE_DATA_VERSION)

        # are we providing month and/or year to date rain, default is no we are
        # not
        self.mtd_rain = to_bool(rtgd_config_dict.get('mtd_rain', False))
        self.ytd_rain = to_bool(rtgd_config_dict.get('ytd_rain', False))
        # initialise some properties if we are providing month and/or year to
        # date rain
        if self.mtd_rain:
            self.month_rain = None
        if self.ytd_rain:
            self.year_rain = None

        # obtain an object for exporting gauge-data.txt if required, if export
        # not required property will be set to None
        self.exporter = self.export_factory(rtgd_config_dict,
                                            self.rtgd_path_file)

        # notify the user of a couple of things that we will do
        # frequency of generation
        if self.min_interval is None:
            _msg = "'%s' wil be generated. "\
                       "min_interval is None" % self.rtgd_path_file
        elif self.min_interval == 1:
            _msg = "'%s' will be generated. "\
                       "min_interval is 1 second" % self.rtgd_path_file
        else:
            _msg = "'%s' will be generated. "\
                       "min_interval is %s seconds" % (self.rtgd_path_file,
                                                       self.min_interval)
        log.info(_msg)
        # lost contact
        if self.ignore_lost_contact:
            log.info("Sensor contact state will be ignored")

    @staticmethod
    def export_factory(rtgd_config_dict, rtgd_path_file):
        """Factory method to produce an object to export gauge-data.txt."""

        exporter = None
        # do we have a legacy remote_server_url setting or a HttpPost stanza
        if 'HttpPost' in rtgd_config_dict or 'remote_server_url' in rtgd_config_dict:
            exporter = 'httppost'
        elif 'Rsync' in rtgd_config_dict:
            exporter = 'rsync'
        exporter_class = EXPORTERS.get(exporter) if exporter else None
        if exporter_class is None:
            # We have no exporter specified or otherwise lacking the necessary
            # config. Log this and return None which will result in nothing
            # being exported (only saving of gauge-data.txt locally).
            log.info("gauge-data.txt will not be exported.")
            exporter_object = None
        else:
            # get the exporter object
            exporter_object = exporter_class(rtgd_config_dict, rtgd_path_file)
        return exporter_object

    def run(self):
        """Collect packets from the rtgd queue and manage their processing.

        Now that we are in a thread get a manager for our db so we can
        initialise our forecast and day stats. Once this is done we wait for
        something in the rtgd queue.
        """

        # would normally do this in our objects __init__ but since we are are
        # running in a thread we need to wait until the thread is actually
        # running before getting db managers

        try:
            # get a db manager
            self.db_manager = weewx.manager.open_manager(self.manager_dict)

            # initialise the time of last rain
            self.last_rain_ts = self.calc_last_rain_stamp()

            # get a windrose to start with since it is only on receipt of an
            # archive record
            self.rose = calc_windrose(int(time.time()),
                                      self.db_manager,
                                      self.wr_period,
                                      self.wr_points)
            if weewx.debug == 2:
                log.debug("windrose data calculated")
            elif weewx.debug >= 3:
                log.debug("windrose data calculated: %s" % (self.rose,))
            # setup our loop cache and set some starting wind values
            _ts = self.db_manager.lastGoodStamp()
            if _ts is not None:
                _rec = self.db_manager.getRecord(_ts)
            else:
                _rec = {'usUnits': None}
            # save the windSpeed value to use as our archive period average, this
            # needs to be a ValueTuple since we may need to convert units
            if 'windSpeed' in _rec:
                self.windSpeedAvg_vt = weewx.units.as_value_tuple(_rec, 'windSpeed')

            # now run a continuous loop, waiting for records to appear in the rtgd
            # queue then processing them.
            while True:
                # inner loop to monitor the queues
                while True:
                    # If we have a result queue check to see if we have received
                    # any forecast data. Use get_nowait() so we don't block the
                    # rtgd control queue. Wrap in a try..except to catch the error
                    # if there is nothing in the queue.
                    if self.result_queue:
                        try:
                            # use nowait() so we don't block
                            _package = self.result_queue.get_nowait()
                        except queue.Empty:
                            # nothing in the queue so continue
                            pass
                        else:
                            # we did get something in the queue but was it a
                            # 'forecast' package
                            if isinstance(_package, dict):
                                if 'type' in _package and _package['type'] == 'forecast':
                                    # we have forecast text so log and save it
                                    if weewx.debug >= 2:
                                        log.debug("received forecast text: %s" % _package['payload'])
                                    self.scroller_text = _package['payload']
                    # now deal with the control queue
                    try:
                        # block for one second waiting for package, if nothing
                        # received throw queue.Empty
                        _package = self.control_queue.get(True, 1.0)
                    except queue.Empty:
                        # nothing in the queue so continue
                        pass
                    else:
                        # a None record is our signal to exit
                        if _package is None:
                            return
                        elif _package['type'] == 'archive':
                            if weewx.debug == 2:
                                log.debug("received archive record (%s)" % _package['payload']['dateTime'])
                            elif weewx.debug >= 3:
                                log.debug("received archive record: %s" % _package['payload'])
                            self.process_new_archive_record(_package['payload'])
                            self.rose = calc_windrose(_package['payload']['dateTime'],
                                                      self.db_manager,
                                                      self.wr_period,
                                                      self.wr_points)
                            if weewx.debug == 2:
                                log.debug("windrose data calculated")
                            elif weewx.debug >= 3:
                                log.debug("windrose data calculated: %s" % (self.rose,))
                            continue
                        elif _package['type'] == 'stats':
                            if weewx.debug == 2:
                                log.debug("received stats package")
                            elif weewx.debug >= 3:
                                log.debug("received stats package: %s" % _package['payload'])
                            self.process_stats(_package['payload'])
                            continue
                        elif _package['type'] == 'loop':
                            # we now have a packet to process, wrap in a
                            # try..except so we can catch any errors
                            try:
                                if weewx.debug == 2:
                                    log.debug("received loop packet (%s)" % _package['payload']['dateTime'])
                                elif weewx.debug >= 3:
                                    log.debug("received loop packet: %s" % _package['payload'])
                                self.process_packet(_package['payload'])
                                continue
                            except Exception as e:
                                # Some unknown exception occurred. This is probably
                                # a serious problem. Exit.
                                log.critical("Unexpected exception of type %s" % (type(e),))
                                weeutil.logger.log_traceback(log.debug, 'rtgdthread: **** ')
                                log.critical("Thread exiting. Reason: %s" % (e, ))
                                return
                    # if packets have backed up in the control queue, trim it until
                    # it's no bigger than the max allowed backlog
                    while self.control_queue.qsize() > 5:
                        self.control_queue.get()
        except Exception as e:
            # Some unknown exception occurred. This is probably
            # a serious problem. Exit.
            log.critical("Unexpected exception of type %s" % (type(e), ))
            weeutil.logger.log_traceback(log.debug, 'rtgdthread: **** ')
            log.critical("Thread exiting. Reason: %s" % (e, ))
            return

    def process_packet(self, packet):
        """Process incoming loop packets and generate gauge-data.txt.

        Input:
            packet: dict containing the loop packet to be processed
        """

        # get time for debug timing
        t1 = time.time()

        # generate if we have no minimum interval setting or if minimum
        # interval seconds have elapsed since our last generation
        if self.min_interval is None or (self.last_write + float(self.min_interval)) < time.time():
            if weewx.debug == 2:
                log.debug("received cached loop packet (%s)" % packet['dateTime'])
            elif weewx.debug >= 3:
                log.debug("received cached loop packet: %s" % (packet,))
            # set our lost contact flag if applicable
            self.lost_contact_flag = self.get_lost_contact(packet, 'loop')
            # get a data dict from which to construct our file
            try:
                data = self.calculate(packet)
            except Exception as e:
                weeutil.logger.log_traceback(log.info, 'rtgdthread: **** ')
            else:
                # write to our file
                try:
                    self.write_data(data)
                except Exception as e:
                    weeutil.logger.log_traceback(log.info, 'rtgdthread: **** ')
                else:
                    # set our write time
                    self.last_write = time.time()
                    # export gauge-data.txt if we have an exporter object
                    if self.exporter:
                        self.exporter.export(data)
                    # log the generation
                    if weewx.debug == 2:
                        log.info("gauge-data.txt (%s) generated in %.5f seconds" % (cached_packet['dateTime'],
                                                                                    (self.last_write - t1)))
        else:
            # we skipped this packet so log it
            if weewx.debug == 2:
                log.debug("cached packet (%s) skipped" % packet['dateTime'])

    def process_stats(self, package):
        """Process a stats package.

        Input:
            package: dict containing the stats data to process
        """

        if package is not None:
            for key, value in package.items():
                setattr(self, key, value)

    def write_data(self, data):
        """Write the gauge-data.txt file.

        Takes dictionary of data elements, converts them to JSON format and
        writes them to file. JSON output is sorted by key and any non-critical
        whitespace removed before being written to file. An atomic write to
        file is used to lessen chance of rtgd/web server file access conflict.
        Destination directory is created if it does not exist.

        Inputs:
            data: dictionary of gauge-data.txt data elements
        """

        # make the destination directory, wrapping it in a try block to catch
        # any errors
        try:
            os.makedirs(self.rtgd_path)
        except OSError as error:
            # raise if the error is anything other than the dir already exists
            if error.errno != errno.EEXIST:
                raise
        # now write to temporary file
        with open(self.rtgd_path_file_tmp, 'w') as f:
            json.dump(data, f, separators=(',', ':'), sort_keys=True)
        # and copy the temporary file to our destination
        os.rename(self.rtgd_path_file_tmp, self.rtgd_path_file)

    def get_field_value(self, field, packet):
        """Obtain the value for an output field."""

        # prime our result
        result = None
        # get the map for this field
        this_field_map = self.field_map.get(field)
        # do we know about this field and do we have a source?
        if this_field_map is not None and this_field_map.get('source') is not None:
            # we have a source
            source = this_field_map['source']
            # get a few things about our result:
            # unit group
            result_group = this_field_map['group'] if 'group' in this_field_map else _getUnitGroup(source)
            # result units
            result_units = self.units_dict[result_group]
            # do we have an aggregate
            if this_field_map.get('aggregate') is not None and this_field_map.get('aggregate_period') is not None:
                # We have an aggregate. Aggregates we know about are min, max,
                # sum, last and trend.
                agg = this_field_map['aggregate'].lower()
                aggregate_period = this_field_map['aggregate_period'].lower()
                # Trend requires ome special processing so pull it out first.
                if agg == 'trend':
                    try:
                        trend_period = int(aggregate_period)
                    except TypeError:
                        trend_period = 3600
                    grace_period = int(this_field_map.get('grace', 300))
                    # obtain the current value as a ValueTuple
                    _current_vt = as_value_tuple(packet, source)
                    # calculate the trend
                    _trend = calc_trend(obs_type=source,
                                        now_vt=_current_vt,
                                        target_units=result_units,
                                        db_manager=self.db_manager,
                                        then_ts=packet['dateTime'] - trend_period,
                                        grace=grace_period)
                    # if the trend result is None use the specified default
                    if _trend is None:
                        _trend = convert(this_field_map['default'], result_units).value
                    result = this_field_map['format'] % _trend
                if aggregate_period == 'day':
                    # aggregate since start of today
                    # is it an aggregate that has units?
                    if agg in ('min', 'max', 'last', 'sum'):
                        # it has units so obtain as a ValueTuple, convert as
                        # required and check for None
                        _raw_vt = ValueTuple(getattr(self.buffer[source], agg),
                                             self.packet_unit_dict[source]['units'],
                                             self.packet_unit_dict[source]['group'])
                        # convert to the output units
                        _conv_raw = convert(_raw_vt, result_units).value
                        if _conv_raw is None:
                            _conv_raw = convert(this_field_map['default'], result_units).value
                        result = this_field_map['format'] % _conv_raw
                    elif agg in ('mintime', 'maxtime', 'lasttime'):
                        # its a time so get the time as a localtime and format
                        _raw = time.localtime(getattr(self.buffer[source], agg))
                        result = time.strftime(this_field_map['format'], _raw)
                    else:
                        pass
                else:
                    # afraid we don't know what to do
                    pass
            else:
                # no aggregate so get the value from the packet as a ValueTuple
                if source in packet:
                    _raw_vt = as_value_tuple(packet, source)
                else:
                    _raw_vt = ValueTuple(None, result_units, _getUnitGroup(source))
                # convert to the output units
                _conv_raw = convert(_raw_vt, result_units).value
                if _conv_raw is None:
                    _conv_raw = convert(this_field_map['default'], result_units).value
                result = this_field_map['format'] % _conv_raw
        return result

    def get_packet_units(self, packet):
        """Given a packet obtain unit details for each field map source."""

        packet_unit_dict = {}
        packet_unit_system = packet['usUnits']
        for field, field_map in self.field_map.items():
            source = field_map['source']
            if source not in packet_unit_dict:
                (units, unit_group) = getStandardUnitType(packet_unit_system,
                                                          source)
                packet_unit_dict[source] = {'units': units,
                                            'group': unit_group}
        # add in units and group details for fields windSpeed and rain to
        # facilitate non-field map based field calculations
        for source in ('windSpeed', 'rain'):
            if source not in packet_unit_dict:
                (units, unit_group) = getStandardUnitType(packet_unit_system,
                                                          source)
                packet_unit_dict[source] = {'units': units,
                                            'group': unit_group}
        return packet_unit_dict

    def calculate(self, packet):
        """Construct a data dict for gauge-data.txt.

        Input:
            packet: loop data packet

        Returns:
            Dictionary of gauge-data.txt data elements.
        """

        # obtain the timestamp for the current packet
        ts = packet['dateTime']
        # obtain a dict of units and unit group for each source in the field map
        self.packet_unit_dict = self.get_packet_units(packet)
        # construct a dict to hold our results
        data = dict()

        # obtain 10 minute average wind direction
        avg_bearing_10 = self.buffer['wind'].history_vec_avg.dir

        # First we populate all non-field map calculated fields and then
        # iterate over the field map populating the field map based fields.
        # Populating the fields in this order allows the user to override the
        # content of a non-field map based field (eg 'rose').

        # timeUTC - UTC date/time in format YYYY,mm,dd,HH,MM,SS
        data['timeUTC'] = datetime.datetime.utcfromtimestamp(ts).strftime("%Y,%m,%d,%H,%M,%S")
        # date - date in (default) format Y.m.d HH:MM
        data['date'] = time.strftime(self.date_format, time.localtime(ts))
        # dateFormat - date format
        data['dateFormat'] = self.date_format.replace('%', '')
        # SensorContactLost - 1 if the station has lost contact with its remote
        # sensors "Fine Offset only" 0 if contact has been established
        data['SensorContactLost'] = self.flag_format % self.lost_contact_flag
        # tempunit - temperature units - C, F
        data['tempunit'] = UNITS_TEMP[self.temp_group]
        # windunit -wind units - m/s, mph, km/h, kts
        data['windunit'] = UNITS_WIND[self.wind_group]
        # pressunit - pressure units - mb, hPa, in
        data['pressunit'] = UNITS_PRES[self.pres_group]
        # rainunit - rain units - mm, in
        data['rainunit'] = UNITS_RAIN[self.rain_group]
        # cloudbaseunit - cloud base units - m, ft
        data['cloudbaseunit'] = UNITS_CLOUD[self.alt_group]

        # TODO. pressL and pressH need to be refactored to use a field map
        # pressL - all time low barometer
        if self.min_barometer is not None:
            press_l_vt = ValueTuple(self.min_barometer,
                                    self.packet_unit_dict['barometer']['units'],
                                    self.packet_unit_dict['barometer']['group'])
        else:
            press_l_vt = ValueTuple(850, 'hPa', self.packet_unit_dict['barometer']['group'])
        press_l = convert(press_l_vt, self.pres_group).value
        data['pressL'] = self.pres_format % press_l
        # pressH - all time high barometer
        if self.max_barometer is not None:
            press_h_vt = ValueTuple(self.max_barometer,
                                    self.packet_unit_dict['barometer']['units'],
                                    self.packet_unit_dict['barometer']['group'])
        else:
            press_h_vt = ValueTuple(1100, 'hPa', self.packet_unit_dict['barometer']['group'])
        press_h = convert(press_h_vt, self.pres_group).value
        data['pressH'] = self.pres_format % press_h

        # domwinddir - Today's dominant wind direction as compass point
        dom_dir = self.buffer['wind'].day_vec_avg.dir
        data['domwinddir'] = degree_to_compass(dom_dir)

        # WindRoseData -
        data['WindRoseData'] = self.rose

        # hourlyrainTH - Today's highest hourly rain
        # FIXME. Need to determine hourlyrainTH
        data['hourlyrainTH'] = "0.0"

        # ThourlyrainTH - time of Today's highest hourly rain
        # FIXME. Need to determine ThourlyrainTH
        data['ThourlyrainTH'] = "00:00"

        # LastRainTipISO - date and time of last rainfall
        if self.last_rain_ts is not None:
            _last_rain_tip_iso = time.strftime(self.date_format,
                                               time.localtime(self.last_rain_ts))
        else:
            _last_rain_tip_iso = "1/1/1900 00:00"
        data['LastRainTipISO'] = _last_rain_tip_iso

        # wspeed - wind speed (average)
        # obtain the average wind speed from the buffer
        _wspeed = self.buffer['windSpeed'].history_avg(ts=ts, age=600)
        # put into a ValueTuple so we can convert
        wspeed_vt = ValueTuple(_wspeed,
                               self.packet_unit_dict['windSpeed']['units'],
                               self.packet_unit_dict['windSpeed']['group'])
        # convert to output units
        wspeed = convert(wspeed_vt, self.wind_group).value
        # handle None values
        wspeed = wspeed if wspeed is not None else 0.0
        data['wspeed'] = self.wind_format % wspeed

        # wgust - 10 minute high gust
        # first look for max windGust value in the history, if windGust is not
        # in the buffer then use windSpeed, if no windSpeed then use 0.0
        if 'windGust' in self.buffer:
            wgust = self.buffer['windGust'].history_max(ts, age=600).value
        elif 'windSpeed' in self.buffer:
            wgust = self.buffer['windSpeed'].history_max(ts, age=600).value
        else:
            wgust = 0.0
        # put into a ValueTuple so we can convert
        wgust_vt = ValueTuple(wgust,
                              self.packet_unit_dict['windSpeed']['units'],
                              self.packet_unit_dict['windSpeed']['group'])
        # convert to output units
        wgust = convert(wgust_vt, self.wind_group).value
        data['wgust'] = self.wind_format % wgust

        # bearing - wind bearing (degrees)
        bearing = packet['windDir']
        bearing = bearing if bearing is not None else self.last_dir
        # save this bearing to use next time if there is no windDir, this way
        # our wind dir needle will always how the last non-None windDir rather
        # than return to 0
        self.last_dir = bearing
        data['bearing'] = self.dir_format % bearing

        # avgbearing - 10-minute average wind bearing (degrees)
        data['avgbearing'] = self.dir_format % avg_bearing_10 if avg_bearing_10 is not None else self.dir_format % 0.0

        # BearingRangeFrom10 - The 'lowest' bearing in the last 10 minutes
        # BearingRangeTo10 - The 'highest' bearing in the last 10 minutes
        # (or as configured using AvgBearingMinutes in cumulus.ini), rounded
        # down to nearest 10 degrees
        if avg_bearing_10 is not None:
            # First obtain a list of wind direction history over the last
            # 10 minutes, but we want the direction to be in -180 to
            # 180 degrees range rather than from 0 to 360 degrees. Also the
            # values must be relative to the 10 minute average wind direction.
            # Wrap in a try.except just in case.
            try:
                _offset_dir = [self.to_plusminus(obs.value.dir-avg_bearing_10) for obs in self.buffer['wind'].history]
            except (TypeError, ValueError):
                # if we strike an error then return 0 for both results
                bearing_range_from_10 = 0
                bearing_range_to_10 = 0
            # Now find the min and max values and transpose back to the 0 to
            # 360 degrees range relative to North (0 degrees). Wrap in a
            # try..except just in case.
            try:
                bearing_range_from_10 = self.to_threesixty(min(_offset_dir) + avg_bearing_10)
                bearing_range_to_10 = self.to_threesixty(max(_offset_dir) + avg_bearing_10)
            except TypeError:
                # if we strike an error then return 0 for both results
                bearing_range_from_10 = 0
                bearing_range_to_10 = 0
        else:
            bearing_range_from_10 = 0
            bearing_range_to_10 = 0
        # store the formatted results
        data['BearingRangeFrom10'] = self.dir_format % bearing_range_from_10
        data['BearingRangeTo10'] = self.dir_format % bearing_range_to_10

        # forecast - forecast text
        _text = self.scroller_text if self.scroller_text is not None else ''
        # format the forecast string, we might get a UnicodeDecode error, be
        # prepared to catch it
        try:
            data['forecast'] = time.strftime(_text, time.localtime(ts))
        except UnicodeEncodeError:
            # FIXME. Possible unicode/bytes issue
            data['forecast'] = time.strftime(_text.encode('ascii', 'ignore'), time.localtime(ts))
        # version - weather software version
        data['version'] = '%s' % weewx.__version__
        # build -
        data['build'] = ''
        # ver - gauge-data.txt version number
        data['ver'] = self.version
        # month to date rain, only calculate if we have been asked
        # TODO. Check this, particularly usage of buffer['rain'].sum
        if self.mtd_rain:
            if self.month_rain is not None:
                rain_m = convert(self.month_rain, self.rain_group).value
                rain_b_vt = ValueTuple(self.buffer['rain'].sum,
                                       self.packet_unit_dict['rain']['units'],
                                       self.packet_unit_dict['rain']['group'])
                rain_b = convert(rain_b_vt, self.rain_group).value
                if rain_m is not None and rain_b is not None:
                    rain_m = rain_m + rain_b
                else:
                    rain_m = 0.0
            else:
                rain_m = 0.0
            data['mrfall'] = self.rain_format % rain_m
        # year to date rain, only calculate if we have been asked
        # TODO. Check this, particularly usage of buffer['rain'].sum
        if self.ytd_rain:
            if self.year_rain is not None:
                rain_y = convert(self.year_rain, self.rain_group).value
                rain_b_vt = ValueTuple(self.buffer['rain'].sum,
                                       self.packet_unit_dict['rain']['units'],
                                       self.packet_unit_dict['rain']['group'])
                rain_b = convert(rain_b_vt, self.rain_group).value
                if rain_y is not None and rain_b is not None:
                    rain_y = rain_y + rain_b
                else:
                    rain_y = 0.0
            else:
                rain_y = 0.0
            data['yrfall'] = self.rain_format % rain_y

        # now populate all fields in the field map
        for field in self.field_map:
            data[field] = self.get_field_value(field, packet)
        return data

    def process_new_archive_record(self, record):
        """Control processing when new a archive record is presented."""

        # set our lost contact flag if applicable
        self.lost_contact_flag = self.get_lost_contact(record, 'archive')
        # save the windSpeed value to use as our archive period average
        if 'windSpeed' in record:
            self.windSpeedAvg_vt = weewx.units.as_value_tuple(record, 'windSpeed')
        else:
            self.windSpeedAvg_vt = ValueTuple(None, 'km_per_hour', 'group_speed')

    def calc_last_rain_stamp(self):
        """Calculate the timestamp of the last rain.

        Searching a large archive for the last rainfall could be time consuming
        so first search the daily summaries for the day of last rain and then
        search that day for the actual timestamp.
        """

        _row = self.db_manager.getSql("SELECT MAX(dateTime) FROM archive_day_rain WHERE sum > 0")
        last_rain_ts = _row[0]
        # now limit our search on the archive to the day concerned, wrap in a
        # try statement just in case
        if last_rain_ts is not None:
            # We have a day so get a TimeSpan for the day containing
            # last_rain_ts. last_rain_ts will be set to midnight at the start
            # of a day (daily summary requirement) but in the archive this ts
            # would belong to the previous day, so add 1 second and obtain the
            # TimeSpan for the archive day containing that ts.
            last_rain_tspan = weeutil.weeutil.archiveDaySpan(last_rain_ts+1)
            try:
                _row = self.db_manager.getSql("SELECT MAX(dateTime) FROM archive "
                                              "WHERE rain > 0 AND dateTime > ? AND dateTime <= ?",
                                              last_rain_tspan)
                last_rain_ts = _row[0]
            except (IndexError, TypeError):
                last_rain_ts = None
        return last_rain_ts

    def get_lost_contact(self, rec, packet_type):
        """Determine is station has lost contact with sensors."""

        # default to lost contact = False
        result = False
        # if we are not ignoring the lost contact test do the check
        if not self.ignore_lost_contact:
            if ((packet_type == 'loop' and self.station_type in LOOP_STATIONS) or
                    (packet_type == 'archive' and self.station_type in ARCHIVE_STATIONS)):
                _v = STATION_LOST_CONTACT[self.station_type]['value']
                try:
                    result = rec[STATION_LOST_CONTACT[self.station_type]['field']] == _v
                except KeyError:
                    log.debug("KeyError: Could not determine sensor contact state")
                    result = True
        return result

    @staticmethod
    def to_plusminus(val):
        """Map a 0 to 360 degree direction to -180 to +180 degrees."""

        if val is not None and val > 180:
            return val - 360
        else:
            return val

    @staticmethod
    def to_threesixty(val):
        """Map a -180 to +180 degrees direction to 0 to 360 degrees."""

        if val is not None and val < 0:
            return val + 360
        else:
            return val


# ============================================================================
#                            Utility Functions
# ============================================================================

def degree_to_compass(x):
    """Convert degrees to ordinal compass point.

    Input:
        x: degrees

    Returns:
        Corresponding ordinal compass point from COMPASS_POINTS. Can return
        None.
    """

    if x is None:
        return None
    idx = int((x + 11.25) / 22.5)
    return COMPASS_POINTS[idx]


def calc_trend(obs_type, now_vt, target_units, db_manager, then_ts, grace=0):
    """ Calculate change in an observation over a specified period.

    Inputs:
        obs_type:     database field name of observation concerned
        now_vt:       value of observation now (ie the finishing value)
        target_units: units our returned value must be in
        db_manager:   manager to be used
        then_ts:      timestamp of start of trend period
        grace:        the largest difference in time when finding the then_ts
                      record that is acceptable

    Returns:
        Change in value over trend period. Can be positive, 0, negative or
        None. Result will be in 'group' units.
    """

    if now_vt.value is None:
        return None
    then_record = db_manager.getRecord(then_ts, grace)
    if then_record is None:
        return None
    else:
        if obs_type not in then_record:
            return None
        else:
            then_vt = weewx.units.as_value_tuple(then_record, obs_type)
            now = convert(now_vt, target_units).value
            then = convert(then_vt, target_units).value
            return now - then


def calc_windrose(now, db_manager, period, points):
    """Calculate a SteelSeries Weather Gauges windrose array.

    Calculate an array representing the 'amount of wind' from each of the 8 or
    16 compass points. The value for each compass point is determined by
    summing the archive windSpeed values for wind from that compass point over
    the period concerned. Resulting values are rounded to one decimal point.

    Inputs:
        db_manager: A manager object for the database to be used.
        period:     Calculate the windrose using the last period (in
                    seconds) of data in the archive.
        points:     The number of compass points to use, normally 8 or 16.

    Return:
        List containing windrose data with 'points' elements.
    """

    # initialise our result
    rose = [0.0 for x in range(points)]
    # get the earliest ts we will use
    ts = now - period
    # determine the factor to be used to divide numerical windDir into
    # cardinal/ordinal compass points
    angle = 360.0/points
    # create an interpolation dict for our query
    inter_dict = {'table_name': db_manager.table_name,
                  'ts': ts,
                  'angle': angle}
    # the query to be used
    windrose_sql = "SELECT ROUND(windDir/%(angle)s),sum(windSpeed) "\
                   "FROM %(table_name)s WHERE dateTime>%(ts)s "\
                   "GROUP BY ROUND(windDir/%(angle)s)"

    # we expect at least 'points' rows in our result so use genSql
    for _row in db_manager.genSql(windrose_sql % inter_dict):
        # for windDir==None we expect some results with None, we can ignore
        # those
        if _row is None or None in _row:
            pass
        else:
            # Because of the structure of the compass and the limitations in
            # SQL maths our 'North' result will be returned in 2 parts. It will
            # be the sum of the '0' group and the 'points' group.
            if int(_row[0]) != int(points):
                rose[int(_row[0])] += _row[1]
            else:
                rose[0] += _row[1]
    # now  round our results and return
    return [round(x, 1) for x in rose]


# available scroller text block classes
EXPORTERS = {'httppost': HttpPostExport,
             'rsync': RsyncExport}
