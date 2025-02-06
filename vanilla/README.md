# Vanilla Baseline

Vanilla baseline answers the queries from the raw, un-indexed data files. 

We will the run the baseline on cs-u-spatial-514.cs.umn.edu.

We have raw data on cs-u-spatial-514.cs.umn.edu:/era5/raw/2m_temperature

Available data and naming convention:
```
$ cd /era5/raw/2m_temperature
$ ll
total 679G
-rw-rw-r-- 1 huan1531 iharp 17G Jul 31  2024 2m_temperature-1984.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 31  2024 2m_temperature-1985.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1986.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1987.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1988.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1989.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1990.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1991.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1992.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1993.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1994.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1995.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1996.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1997.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1998.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 30  2024 2m_temperature-1999.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2000.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2001.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2002.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2003.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2004.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2005.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2006.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2007.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2008.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2009.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2010.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2011.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2012.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 26  2024 2m_temperature-2013.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2014.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2015.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2016.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2017.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2018.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2019.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2020.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2021.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2022.nc
-rw-rw-r-- 1 huan1531 iharp 17G Jul 24  2024 2m_temperature-2023.nc
```