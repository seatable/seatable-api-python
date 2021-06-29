import datetime
import calendar

DATETIME_FORMAT = {
    "ymd": "%Y-%m-%d",
    "ymd_h": "%Y-%m-%d %H",
    "ymd_hm": "%Y-%m-%d %H:%M",
    "ymd_hms": "%Y-%m-%d %H:%M:%S"
}


class DateUtils(object):

    def _isoformat(self, d):
        if not isinstance(d, (datetime.date, datetime.datetime)):
            raise ValueError('datetime type error')
        return d.isoformat()

    def _get_format_type(self, date_str):
        spli = date_str.split(" ")
        format_type = ''
        if len(spli) == 1:
            format_type = 'ymd'
        if len(spli) == 2:
            _, hms = spli
            spli_hms = hms.split(":")
            if len(spli_hms) == 3:
                format_type = 'ymd_hms'
            if len(spli_hms) == 2:
                format_type = 'ymd_hm'
            if len(spli_hms) == 1:
                format_type = 'ymd_h'
        return format_type

    def _str2datetime(self, date_str):
        format_type = self._get_format_type(date_str)
        if not format_type:
            raise ValueError('invalid time format')
        format_type_str = DATETIME_FORMAT.get(format_type)
        return datetime.datetime.strptime(date_str, format_type_str)

    def _delta(self, count, unit):
        from dateutil.relativedelta import relativedelta
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
        return self._isoformat(dt)

    def dateadd(self, date_str, count, unit):
        dt = self._str2datetime(date_str)
        delta = self._delta(count, unit)
        if not delta:
            raise ValueError('invalid delta')
        return self._isoformat(dt + delta)

    def datediff(self, start, end, unit='S'):
        """
        计算两个日期之间相隔的秒数、天数、月数或年数。
        参数 unit 可以为 S, Y, M, D, YD, YM, MD中的一个。
        YD, startDate 与 endDate 的日期部分之差, 忽略日期中的年份。
        YM, startDate 与 endDate 之间月份之差, 忽略日期中的天和年份。
        MD, startDate 与 endDate 之间天数之差, 忽略日期中的月份和年份。

        dateDif("2020-01-01", "2020-01-02") = 86400
        dateDif("2020-01-01", "2020-01-02", "S") = 86400
        dateDif("2018-01-01", "2020-01-01", "Y") = 2
        dateDif("2020-11-11", "2020-12-12", "M") = 1
        dateDif("2019-06-01", "2020-08-15", "D") = 441
        dateDif("2019-06-01", "2020-08-15", "YD") = 75
        dateDif("2019-06-01", "2020-08-15", "YM") = 2
        dateDif("2019-06-01", "2020-08-15", "MD") = 14
        :param start:
        :param end:
        :param unit:
        :return:
        """

        from dateutil.relativedelta import relativedelta
        from dateutil import rrule

        dt_start = self._str2datetime(start)
        dt_end = self._str2datetime(end)

        if unit == 'S':
            delta = (dt_end - dt_start).days * 3600 * 24

        elif unit in ['D', 'M', 'Y', 'H']:
            freq = {
                'D': rrule.DAILY,
                'M': rrule.MONTHLY,
                'Y': rrule.YEARLY,
                'H': rrule.HOURLY
            }.get(unit)
            delta = rrule.rrule(
                freq, dtstart=dt_start, until=dt_end,
            ).count()

        elif unit == 'MD':
            delta = dt_end.day - dt_start.day
        elif unit == 'YM':
            delta = dt_end.month - dt_start.month
        elif unit == 'YD':
            delta = relativedelta(dt_start, dt_end).days
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
        dt = self._str2datetime(time_str)
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
        return self._str2datetime(time_str).day

    def days(self, time_start, time_end):
        """
        return the interval of two given date by days
        """

        return self.datediff(time_start, time_end, unit='D')

    def hour(self, time_str):
        return self._str2datetime(time_str).hour

    def hours(self, time_start, time_end):
        """
        return the interval of two given date by hours
        """
        return self.datediff(time_start, time_end, unit='H')

    def minute(self, time_str):
        return self._str2datetime(time_str).minute

    def month(self, time_str):
        return self._str2datetime(time_str).month

    def months(self, time_start, time_end):
        """
        return the interval of two given date by months
        """
        return self.datediff(time_start, time_end, unit='M')

    def second(self, time_str):
        return self._str2datetime(time_str).second

    def now(self):
        return self._isoformat(datetime.datetime.now())

    def today(self):
        return self._isoformat(datetime.datetime.now().date())

    def year(self, time_str):
        return self._str2datetime(time_str).year

    def weekday(self, time_str):
        """
        return the number of a day in a week, Monday=0, Tue=1, ..., Sun=6
        :param time_str:
        :return:
        """
        return datetime.datetime.weekday(self._str2datetime(time_str))

    def isoweekday(self, time_str):
        """
        return the iso number of a day in a week, Mon=1, Tue=2, ..., Sun=7
        """
        return datetime.datetime.isoweekday(self._str2datetime(time_str))

    def weeknum(self, time_str):
        """
        return the week number in a year by defining the first week contains the first
        week of a year, xxxx-01-01
        """
        dt = self._str2datetime(time_str)
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
        return self._str2datetime(time_str).isocalendar()[1]


dateutils = DateUtils()
