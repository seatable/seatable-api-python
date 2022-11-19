from seatable_api import dateutils

# test the functions of date utils
time_str = "2020-6-15"
time_str_s = "2020-6-15 15:23:21"

time_end = "2020-5-3 13:13:13"
time_start = "2019-6-3 20:1:12"

# 1. dateutils.date
print(dateutils.date(2020, 5, 16))  # 2020-05-16

# 2. dateutils.dateadd
print(dateutils.dateadd(time_str, -2, 'years')) # 2018-06-15
print(dateutils.dateadd(time_str, 3, 'months')) # 2020-09-15
print(dateutils.dateadd(time_str_s, 44, 'minutes')) # 2020-06-15 16:07:21
print(dateutils.dateadd(time_str_s, 1000, 'days')) # 2023-03-12 15:23:21
print(dateutils.dateadd(time_str_s, 3, 'weeks')) # 2020-07-06 15:23:21
print(dateutils.dateadd(time_str_s, -3, 'hours')) # 2020-06-15 12:23:21
print(dateutils.dateadd(time_str_s, 3, 'seconds')) # 2020-06-15 15:23:24

# 3. dateutils.datediff
print(dateutils.datediff(start=time_start, end=time_end, unit='S')) # seconds 28919521
print(dateutils.datediff(start=time_start, end=time_end, unit='Y')) # years 0
print(dateutils.datediff(start=time_start, end=time_end, unit='D')) # days 335
print(dateutils.datediff(start=time_start, end=time_end, unit='H')) # hours 8033
print(dateutils.datediff(start=time_start, end=time_end, unit='M')) # months 11
print(dateutils.datediff(start=time_start, end=time_end, unit='YM')) # relative-month 11
print(dateutils.datediff(start=time_start, end=time_end, unit='MD')) # relative-days 0

# 4. dateutils.day
print(dateutils.day(time_str_s)) # 15

# 5. dateutils.days
print(dateutils.days(time_start, time_end)) # 334

# 6. dateutils.hour
print(dateutils.hour(time_start)) # 20

# 7. dateutils.hours
print(dateutils.hours(time_start, time_end)) # 8033

# 8. dateutils.minute
print(dateutils.minute(time_start)) # 1

# 9. dateutils.month
print(dateutils.month(time_str_s)) # 6

# 10. dateutils.months
print(dateutils.months(time_start, time_end)) # 11

# 11. dateutils.second
print(dateutils.second(time_str_s)) # 21

# 12. dateutils.now
print(dateutils.now())

# 13. dateutils.today
print(dateutils.today()) # 2021-06-28

# 14. dateutils.year
print(dateutils.year(time_start)) # 2019

# 15. dateutils.weekday
print(dateutils.isoweekday(time_start)) #1

# 16. dateutils.weeknum
print(dateutils.weeknum(time_start)) # 23


# 17. dateutils.isoweeknum
print(dateutils.isoweeknum(time_end)) # 18


# 18. dateutils.eomonth
print(dateutils.eomonth('2020-3-25', months=-1)) # 2021-02-29
print(dateutils.eomonth('2021-3-25', months=1)) # 2021-04-30


print(dateutils.isoweeknum('2012-1-2')) # 1, monday
print(dateutils.weeknum('2012-1-2')) # 2, monday

# 19. dateutils.isomonth
print(dateutils.isomonth('2012-1-2'))

# 20. others
dt_now = dateutils.now()  # 2022-02-07 09:49:14
dt_10_days = dateutils.dateadd(dt_now, 10) # 2022-02-17 09:49:14
dt_month_10_days = dateutils.month(dt_10_days) # 2
dt_10_days_before = dateutils.dateadd(dt_now, -10)
date_df = dateutils.datediff(dt_10_days_before, dt_10_days, unit="D") # 20

time_str = "2022-07-17T18:15:41.106-05:00"
time_day = dateutils.day(time_str) # 17
time_month = dateutils.month(time_str) # 7
time_year = dateutils.year(time_str) # 2022
res = dateutils.dateadd(dateutils.dateadd(dateutils.now(), 10), 10)

print(dateutils.to_quarter(time_str)) # <DateQuarter-2022,3Q>
time_str2 = "2022-07-28"

q1 = dateutils.to_quarter(time_str)
q2 = dateutils.to_quarter(time_str2)
print(q1 < time_str2) # False
print(q1 + 1) # <DateQuarter-2022,4Q>


time_str = "2022-07-17"
q1 = dateutils.to_quarter(time_str)
print(q1.year) # 2022
print(q1.quarter) # 3

print(q1.start_date) # 2022-07-01
print(q1.end_date) # 2022-09-30

print("2022-8-28" in q1) # True

print(list(dateutils.quarters_within("2021-03-28", "2022-07-17", include_last=True))) # [<DateQuarter-2021,1Q>, ...,<DateQuarter-2022,3Q>]

print(dateutils.quarter_from_yq(2022, 4)) # <DateQuarter-2022,4Q>
print(dateutils.quarter_from_ym(2022, 4)) # <DateQuarter-2022,2Q>