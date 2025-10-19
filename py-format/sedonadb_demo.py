#!/usr/bin/env python
# coding: utf-8

# ## sedonadb_demo
# 
# null

# ![image-alt-text](https://sedona.apache.org/sedonadb/latest/image/sedonadb-architecture.svg)

# In[ ]:


get_ipython().system('pip install "apache-sedona[db]"')


# In[16]:


import sedona.db

sd = sedona.db.connect()
sd.sql("SELECT ST_Point(0, 1) as geom").show()


# In[10]:


cities = sd.read_parquet(
    "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet"
)


# In[11]:


cities.show()


# In[12]:


countries = sd.read_parquet(
    "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries_geo.parquet"
)


# In[13]:


countries.show()


# In[14]:


cities.to_view("cities")
countries.to_view("countries")


# In[15]:


# join the cities and countries tables
sd.sql("""
select * from cities
join countries
where ST_Intersects(cities.geometry, countries.geometry)
""").show()

