# tap-kwanko

This is a Singer tap that produces JSON-formatted data following the Singer spec.

This tap:

Pulls raw data from Kwanko's REST API
Extracts the following resources from Kwanko
Sale data > stream : "sale"
Site ID stats > stream : "stats_by_site"
Campain ID stats > stream : "stats_by_campain"
Day stats > stream : "stats_by_day"
Monthly stats > stream : "stats_by_month"


Outputs the schema for each resource
Incrementally pulls data based on the input state
Configuration
This tap requires a config.json which specifies details regarding OAuth 2.0 authentication, a cutoff date for syncing historical data. See config.sample.json for an example. 

To run discovery mode and generate the catalog.json :
› tap-kwanko -c config.json --discover > catalog.json

For each stream you want to run, add the field : 

"selected": true,

Among the schema properties of the stream in the catalog.json. (Just before the property named : "properties":{})

To run tap-kwanko with the configuration file, use this command:

› tap-kwanko -c config.json --catalog catalog.json

Later you can retrieve the data from the last run with : 
› tap-kwanko -c config.json --state state.json --catalog catalog.json

(In dev mode, after running the tap do not forget to edit the state.json in order to put the state written in your logs)
---

Copyright &copy; 2020 Reeport
