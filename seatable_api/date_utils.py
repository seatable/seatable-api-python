import datetime
import calendar
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


DATETIME_FORMAT = {
    "ymd": "%Y-%m-%d",
    "ymd_h": "%Y-%m-%d %H",
    "ymd_hm": "%Y-%m-%d %H:%M",
    "ymd_hms": "%Y-%m-%d %H:%M:%S",
    "ymd_hmsf": "%Y-%m-%d %H:%M:%S.%f",
}


class DateUtils(object):

    def _isoformat(self, d, format_str="%Y-%m-%d"):
        if not isinstance(d, (datetime.date, datetime.datetime)):
            raise ValueError('datetime type error')
        return d.strftime(format_str)

    def _get_format_type(self, date_str):
        spli = date_str.split(" ")
        format_type = ''
        if len(spli) == 1:
            format_type = 'ymd'
        if len(spli) == 2:
            _, hms = spli
            spli_hms = hms.split(":")
            if len(spli_hms) == 3:
                second_str = spli_hms[-1]
                spli_second_str = second_str.split('.')
                if len(spli_second_str) == 2:
                    format_type = 'ymd_hmsf'
                else:
                    format_type = 'ymd_hms'
            if len(spli_hms) == 2:
                format_type = 'ymd_hm'
            if len(spli_hms) == 1:
                format_type = 'ymd_h'
        return format_type

    def _handle_timestr_with_timezone(self, time_str):

        # deal with the timzone info if has timezone

        local_now = datetime.datetime.now()
        utc_now = datetime.datetime.utcnow()
        seconds2utc = (local_now - utc_now).total_seconds()

        dt_obj = parse(time_str)
        if dt_obj.tzinfo:
            seconds_offset = dt_obj.utcoffset().total_seconds()
            delta_seconds = seconds2utc - seconds_offset
            dt_obj = dt_obj.replace(tzinfo=None)
            dt_obj_to_local = dt_obj + datetime.timedelta(seconds=delta_seconds)
            return dt_obj_to_local.strftime("%Y-%m-%d %H:%M:%S")
        return time_str

    def _str2datetime(self, datetime_str):
        datetime_str = self._handle_timestr_with_timezone(datetime_str)
        datetime_str = datetime_str.replace("T", " ")
        format_type = self._get_format_type(datetime_str)
        if not format_type:
            raise ValueError('invalid time format')
        format_type_str = DATETIME_FORMAT.get(format_type)
        datetime_obj = datetime.datetime.strptime(datetime_str, format_type_str)
        return datetime_obj, format_type_str

    def _delta(self, count, unit):
        return {
            'years': relativedelta(years=count),
            'months': relativedelta(months=count),
            'weeks': relativedelta(weeks=count),
            'days': relativedelta(days=count),
            'hours': relativedelta(hours=count),
            'minutes': relativedelta(minutes=count),
            'seconds': relativedelta(seconds=count),
        }.get(unit, {})

    def date(self, year, month, day, *args):
        '''
        ex: date(2021, 1, 1) = 2021-01-01
        :return: iso format of a date
        '''
        dt = datetime.date(year, month, day)
        if args:
            dt = datetime.datetime(year, month, day, *args)
        return self._isoformat(dt)

    def dateadd(self, date_str, count, unit='days'):
        dt, format_str = self._str2datetime(date_str)
        delta = self._delta(count, unit)
        if not delta:
            raise ValueError('invalid delta')
        return self._isoformat(dt + delta, format_str)

    def datediff(self, start, end, unit='S'):
        dt_start, _ = self._str2datetime(start)
        dt_end, _ = self._str2datetime(end)

        if unit == 'S':
            delta = (dt_end - dt_start).total_seconds()
            return int(delta)
        elif unit == 'D':
            delta = (dt_end - dt_start).days
        elif unit == 'H':
            delta_seconds = (dt_end - dt_start).total_seconds()
            return int(delta_seconds / 3600)
        elif unit == 'M':
            dt_start_year, dt_start_month = dt_start.year, dt_start.month
            dt_end_year, dt_end_month = dt_end.year, dt_end.month
            delta = (dt_end_year - dt_start_year) * 12 + (dt_end_month - dt_start_month)
        elif unit == 'Y':
            start_end_delta = (dt_end - dt_start).days
            if start_end_delta < 365:
                delta = 0
            else:
                delta = dt_end.year - dt_start.year
        elif unit == 'MD':
            delta = dt_end.day - dt_start.day
        elif unit == 'YM':
            delta = dt_end.month - dt_start.month
        elif unit == 'YD':
            if dt_end.month < dt_start.month:
                dt_start_new = dt_start.replace(year=dt_end.year - 1)
            else:
                dt_start_new = dt_start.replace(year=dt_end.year)
            delta = (dt_end - dt_start_new).days
        else:
            delta = None
        return delta

    def emonth(self, time_str, direction=1):
        """
        return the last day of the next/last month of given time
        :param time_str:
        :param direction:
        :return:
        """
        dt, _ = self._str2datetime(time_str)
        if direction == 1:
            month_dt = dt.replace(day=28) + datetime.timedelta(days=4) # some day in next month
        elif direction == -1:
            month_dt = dt.replace(day=1) - datetime.timedelta(days=2) # some day in last month
        else:
            raise ValueError('direction invalid.')

        month_dt_year = month_dt.year
        month_dt_month = month_dt.month
        days = calendar.monthrange(month_dt_year, month_dt_month)[1]
        return self._isoformat(datetime.date(month_dt_year, month_dt_month, 1) + datetime.timedelta(days=days-1))

    def day(self, time_str):
        return self._str2datetime(time_str)[0].day

    def days(self, time_start, time_end):
        """
        return the interval of two given date by days
        """
        return self.datediff(time_start, time_end, unit='D')

    def hour(self, time_str):
        return self._str2datetime(time_str)[0].hour

    def hours(self, time_start, time_end):
        """
        return the interval of two given date by hours
        """
        return self.datediff(time_start, time_end, unit='H')

    def minute(self, time_str):
        return self._str2datetime(time_str)[0].minute

    def month(self, time_str):
        return self._str2datetime(time_str)[0].month

    def months(self, time_start, time_end):
        """
        return the interval of two given date by months
        """
        return self.datediff(time_start, time_end, unit='M')

    def second(self, time_str):
        return self._str2datetime(time_str)[0].second

    def now(self):
        return self._isoformat(datetime.datetime.now(), format_str="%Y-%m-%d %H:%M:%S")

    def today(self):
        return self._isoformat(datetime.datetime.now().date())

    def year(self, time_str):
        return self._str2datetime(time_str)[0].year

    def weekday(self, time_str):
        """
        return the number of a day in a week, Monday=0, Tue=1, ..., Sun=6
        :param time_str:
        :return:
        """
        return datetime.datetime.weekday(self._str2datetime(time_str)[0])

    def isoweekday(self, time_str):
        """
        return the iso number of a day in a week, Mon=1, Tue=2, ..., Sun=7
        """
        return datetime.datetime.isoweekday(self._str2datetime(time_str)[0])

    def weeknum(self, time_str):
        """
        return the week number in a year by defining the first week contains the first
        week of a year, xxxx-01-01
        """
        dt, _ = self._str2datetime(time_str)
        dt_year = dt.year
        dt_jan = datetime.datetime(dt_year, 1, 1)
        days_to_second_week = 6 - self.weekday("%s-%s-%s" % (dt_year, 1, 1))
        days_to_start = (dt - dt_jan).days
        if days_to_start <= days_to_second_week:
            return 1
        else:
            return (days_to_start - days_to_second_week) // 7 + 2

    def isoweeknum(self, time_str):
        """
        return the week number by the definition of ISO datetime scheme
        """
        return self._str2datetime(time_str)[0].isocalendar()[1]

    def isomonth(self, time_str):
        year = self.year(time_str)
        month = self.month(time_str)

        date_str = self.date(year, month, 1)
        return date_str[0:-3]



dateutils = DateUtils()
