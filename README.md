# covidcast-nowcast
Nowcasting efforts for SARS-Cov2 within Delphi using the covidcast data

## Utilities

No utilities yet

## Approaches

1. [Case deconvolution](case_deconv) : estimate the case reporting delay distribution from line list data and deconvolve the case counts


## Running Locally
# Capturing Database Metrics
This module contains methods for collecting metrics on the database server under different conditions.

## Running the sensor BD locally

1. [Temporary step until new epidata client is deployed] 
   Copy the [Epidata client](https://github.com/cmu-delphi/delphi-epidata/blob/main/src/client/delphi_epidata.py) 
   into the delphi_covidcast_nowcast/ folder in this.
2. In the _get_historical_data() function in sensor.py, edit the Epidata client's BASE_URL variable to `http://localhost:10080/epidata/api.php`.
   You will need to change this back after the call so any calls to the production covidcast still work.
   ```
    Epidata.BASE_URL = "http://localhost:10080/epidata/api.php"  # add this
    response = Epidata.covidcast_nowcast(data_source=sensor.source,
                                         signals=sensor.signal,
                                         time_type="day",
                                         geo_type=geo_type,
                                         time_values=Epidata.range(start_date, end_date),
                                         geo_value=geo_value,
                                         sensor_names=sensor.name)
    Epidata.BASE_URL = "https://delphi.cmu.edu/epidata/api.php"  # add this
   ```
3. In sensorization/sensor.py, change the Epidata import from `from delphi_epidata import Epidata` to 
   `from ..delphi_epidata import Epidata` to use the local file
5. Using the [Docker tutorials](https://github.com/cmu-delphi/delphi-epidata/blob/main/docs/epidata_development.md),
   install Docker and clone all the relevant repositories to a working directory. Do not build the images yet.
4. Generate or obtain any historical data you would like to use, with sub folders corresponding to the production ingestion structure: 
   `common/covidcast_nowcast/receiving/<data_source>/<filename>`. Put these files in the same working directory. 
   You may want to edit the `_export_csv()` output directory (`receiving_dir`) to this location for convenience, e.g. 
   `receiving_dir="/home/andrew/Documents/docker-delphi/common/covidcast_nowcast/receiving/"`.
   Your working directory should look like this:  
   ```
   > tree . -L 2
     ├── common   
     │    └── covidcast_nowcast  
     └── repos  
     │    ├── delphi  
     │    └── undefx  
    ```
3. Before building the images, you will need to make two edits in the operations repo:  
    a. Edit `repos/delphi/operations/src/secrets.py` by setting `db.host = 'delphi_database_epidata'` and 
    `set db.epi = ('user', 'pass')`, which will match the testing docker image.  
    b. Add `COPY common /common` to `repos/delphi/operations/dev/docker/python/Dockerfile` after copying source files.
6. Continue with the tutorial to complete the following steps:  
    a. Build the `delphi_web`, `delphi_web_epidata`, `delphi_database`, and `delphi_python` images.   
    b. Create the `delphi-net` network.  
    c. Run the database and web server. 
7. Run the acquisition script through the Python docker container to upload the data to the server. If you reupload the 
   same sensor, even if the values change, this will currently fail; You will have to either rename the sensor or 
   delete and restart the database.
   ```
    docker run --rm -i --network delphi-net  delphi_python  python3 -m delphi.epidata.acquisition.covidcast_nowcast.load_sensors
   ```
8. You should now be able to run the nowcast code (outside the docker container) and it will retrieve any stored 
   historical data.
   
Example:

This example can be used to both generate the sensorized data to upload, and also to demonstrate retrieving stored data 
once the upload is done. In the first run, a "No historical results found" message will be printed. Once there is data 
that is retrieved, this message will not appear
```
from delphi_covidcast_nowcast.sensorization import sensor                                                                                                   
from delphi_covidcast_nowcast.data_containers import LocationSeries, SignalConfig               
                                                            
example_sensors = [SignalConfig("fb-survey", "smoothed_cli", "test_sensor")]
example_truth = [LocationSeries("ca", "state", [20210101, 20210102, 20210103, 20210104, 20210105, 20210106, 20210107], [1,5,3,5,7,3,7])]                                                                                     
%time sensor.get_sensors(20210101, 20210108, example_sensors, example_truth, compute_missing=True, use_latest_issue=True, export_data=True)
```
   
