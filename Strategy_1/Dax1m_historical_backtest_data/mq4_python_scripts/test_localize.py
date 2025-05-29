#!/usr/bin/env python3
import pytz
from datetime import datetime

# 1) Define the timezones
tz_chi = pytz.timezone("America/Chicago")
tz_london = pytz.timezone("Europe/London")

# 2) Pick a sample from your raw CSV
#    For example: "01/01/2024 18:15:00" (which means Jan-1 at 18:15 Chicago time)
dt_str = "01/01/2024 18:15:00"

# 3) Parse as naive datetime (dayfirst => dd/mm/yyyy if that’s how your CSV is)
naive = datetime.strptime(dt_str, "%d/%m/%Y %H:%M:%S")

# 4) Localize to Chicago
dt_chi = tz_chi.localize(naive)

# 5) Convert to London
dt_lon = dt_chi.astimezone(tz_london)

print("Raw string:            ", dt_str)
print("Naive datetime object: ", naive)
print("Chicago localized:     ", dt_chi)
print("London time:           ", dt_lon)
