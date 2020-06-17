# National Grid Data Portal API Wrapper


<br>

### Overview

<b>About the Portal</b>
 
The <a href="https://carbon-intensity.github.io/api-definitions/#carbon-intensity-api-v2-0-0">National Grid ESO Carbon Intensity API</a> provides an interface to data on the Carbon Intensity of the UK electricity system at both a national and regional (DNO) level. It was developed as a collaboration between WFF, Environmental Defense Fund, NG ESO & Oxford University. 

This Python wrapper makes it easier to query data from the API and receive back Panda's DataFrames ready for further analysis, as well as simplify the querying procedure itself. If you have any ideas for the module please feel free to contribute!

<br>

The package can be installed using:
```bash
pip install CIDataPortal
```

<br>
<br>

### Module Usage

<b>Getting Started</b>

The module's <i>Wrapper</i> class is the main interface with the API, it can be imported as follows:

```python
from CIDataPortal import Wrapper
```

<br>

To make a query you must first initialise the Wrapper class. You can then use the <i>.query_API()</i> to (by default) retrieve data for todays obsrrved and forecasted carbon intensity. The response is then automatically parsed into a Panda's DataFrame.

```python
wrapper = Wrapper()

df = wrapper.query_API()

df.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>forecast</th>      <th>actual</th>    </tr>  </thead>  <tbody>    <tr>      <th>2020-06-17 00:00:00+00:00</th>      <td>263</td>      <td>265.0</td>    </tr>    <tr>      <th>2020-06-17 00:30:00+00:00</th>      <td>259</td>      <td>263.0</td>    </tr>    <tr>      <th>2020-06-17 01:00:00+00:00</th>      <td>259</td>      <td>262.0</td>    </tr>    <tr>      <th>2020-06-17 01:30:00+00:00</th>      <td>259</td>      <td>262.0</td>    </tr>    <tr>      <th>2020-06-17 02:00:00+00:00</th>      <td>256</td>      <td>264.0</td>    </tr>  </tbody></table>

<br>

It is then trivial to then plot and carry out further analysis with the data, e.g:

```python
wrapper.query_API().plot()
plt.ylabel('gCO2/kWh')
```

<img src="img/example_emissions_forecast.png"></img>

<br>

<b>Advanced Usage</b>

We can also specify the data stream, spatial aggregation level and date range to be returned from the API. Whilst the API limits requests to a maximum of 2-weeks, the Python wrapper automatically handles the splitting of queries and collation of returned data. 

```python
wrapper = Wrapper()

df = wrapper.query_API('2020-01-01',
                       '2020-06-01',
                       level='national', 
                       data_stream='generation')

df.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>biomass</th>      <th>coal</th>      <th>imports</th>      <th>gas</th>      <th>nuclear</th>      <th>other</th>      <th>hydro</th>      <th>solar</th>      <th>wind</th>    </tr>  </thead>  <tbody>    <tr>      <th>2020-01-01 00:00:00+00:00</th>      <td>8.7</td>      <td>2.5</td>      <td>9.5</td>      <td>29.5</td>      <td>25.8</td>      <td>0.5</td>      <td>2.5</td>      <td>0.0</td>      <td>21.0</td>    </tr>    <tr>      <th>2020-01-01 00:30:00+00:00</th>      <td>8.6</td>      <td>2.4</td>      <td>9.3</td>      <td>30.8</td>      <td>25.3</td>      <td>0.4</td>      <td>2.4</td>      <td>0.0</td>      <td>20.8</td>    </tr>    <tr>      <th>2020-01-01 01:00:00+00:00</th>      <td>8.9</td>      <td>2.5</td>      <td>9.6</td>      <td>29.1</td>      <td>26.2</td>      <td>0.5</td>      <td>2.5</td>      <td>0.0</td>      <td>20.7</td>    </tr>    <tr>      <th>2020-01-01 01:30:00+00:00</th>      <td>9.0</td>      <td>2.6</td>      <td>9.8</td>      <td>28.5</td>      <td>26.7</td>      <td>0.5</td>      <td>2.3</td>      <td>0.0</td>      <td>20.6</td>    </tr>    <tr>      <th>2020-01-01 02:00:00+00:00</th>      <td>9.2</td>      <td>2.6</td>      <td>10.0</td>      <td>27.5</td>      <td>27.3</td>      <td>0.5</td>      <td>2.1</td>      <td>0.0</td>      <td>20.8</td>    </tr>  </tbody></table>

<br>