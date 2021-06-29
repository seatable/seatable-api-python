from seatable_api.date_utils import dateutils

# test the functions of date utils

time_str = "2020-6-15"
time_str_s = "2020-6-15 15:23:21"

time_end = "2021-4-5 13:20:00"
time_start = "2019-5-5 17:09:35"

# 1. dateutils.date
print(dateutils.date(2020, 5, 16))  # 2020-05-16

# 2. dateutils.dateadd
print(dateutils.dateadd(time_str, -2, 'years')) # 2018-06-15T00:00:00
print(dateutils.dateadd(time_str, 3, 'months')) # 2020-09-15T00:00:00
print(dateutils.dateadd(time_str_s, 44, 'minutes')) # 2020-06-15T16:07:21

# 3. dateutils.datediff
print(dateutils.datediff(start=time_start, end=time_end, unit='S')) # seconds -60480000
print(dateutils.datediff(start=time_start, end=time_end, unit='Y')) # seconds -60480000
print(dateutils.datediff(start=time_start, end=time_end, unit='D')) # days 700
print(dateutils.datediff(start=time_start, end=time_end, unit='H')) # hours 16797
print(dateutils.datediff(start=time_start, end=time_end, unit='M')) # months 23
print(dateutils.datediff(start=time_start, end=time_end, unit='YD')) # relateve-days -30
print(dateutils.datediff(start=time_start, end=time_end, unit='YM')) # relateve-days -1
print(dateutils.datediff(start=time_start, end=time_end, unit='MD')) # relateve-days 0


# 4. dateutils.day
print(dateutils.day(time_str_s)) # 15

# 5. dateutils.days
print(dateutils.days(time_start, time_end)) # 700

# 6. dateutils.hour
print(dateutils.hour(time_start)) # 17

# 7. dateutils.hours
print(dateutils.hours(time_start, time_end)) # 16797

# 8. dateutils.minute
print(dateutils.minute(time_start)) # 9

# 9. dateutils.month
print(dateutils.month(time_str_s)) # 6

# 10. dateutils.months
print(dateutils.months(time_start, time_end)) # 23

# 11. dateutils.second
print(dateutils.second(time_str_s)) # 21

# 12. dateutils.now
print(dateutils.now()) # 2021-06-28T15:22:39.995855

# 13. dateutils.today
print(dateutils.today()) # 2021-06-28

# 14. dateutils.year
print(dateutils.year(time_start)) # 2019

# 15. dateutils.weekday
print(dateutils.weekday(time_start, ))

# 16. dateutils.weeknum
print(dateutils.weeknum(time_start)) # 19

# 17. dateutils.isoweeknum
print(dateutils.isoweeknum(time_end)) # 14


# 18. dateutils.emonth
print(dateutils.emonth('2020-3-25', direction=-1)) # 2021-02-28
print(dateutils.emonth('2021-3-25', direction=1)) # 2021-04-30


print(dateutils.isoweeknum('2012-1-2')) # 1, monday
print(dateutils.weeknum('2012-1-2')) # 2, monday