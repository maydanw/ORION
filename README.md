
# ORION
### Operational Routine for the Ingest and Output of Networks

This package takes data sets from various sources and converts them into Knowledge Graphs.

Each data source will go through the following pipeline before it can be included in a graph:

1. Fetch (retrieve an original data source) 
2. Parse (convert the data source into KGX files) 
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms) 
4. Supplement (add supplementary knowledge specific to that source)

To build a graph use a Graph Spec yaml file to specify the sources you want.

ORION will automatically run each data source specified through the necessary pipeline. Then it will merge the specified sources into a Knowledge Graph.

### Using ORION

Create a parent directory:
```
mkdir ~/ORION_root
```

Clone the code repository:
```
cd ~/ORION_root
git clone https://github.com/RobokopU24/ORION.git
```

Next create directories where data sources, graphs, and logs will be stored. 

ORION_STORAGE - for storing data sources

ORION_GRAPHS - for storing knowledge graphs

ORION_LOGS - for storing logs

You can do this manually, or use the script indicated below to set up a default configuration.

Option 1: Use this script to create the directories and set the environment variables:
```
cd ~/ORION_root/ORION/
source ./set_up_test_env.sh
```

Option 2: Create three directories and manually set environment variables specifying paths to the locations of those directories.
```
mkdir ~/ORION_root/storage/
export ORION_STORAGE=~/ORION_root/storage/ 

mkdir ~/ORION_root/graphs/
export ORION_GRAPHS=~/ORION_root/graphs/

mkdir ~/ORION_root/logs/
export ORION_LOGS=~/ORION_root/logs/
```

Next create or select a Graph Spec yaml file where the content of knowledge graphs to be built will be specified.

Use either of the following options, but not both:

Note that running the setup script set_up_test_env.sh will perform Option 1 for you.

Option 1: ORION_GRAPH_SPEC - the name of a Graph Spec file located in the graph_specs directory of ORION
```
export ORION_GRAPH_SPEC=testing-graph-spec.yml
```
Option 2: ORION_GRAPH_SPEC_URL - a URL pointing to a Graph Spec file
```
export ORION_GRAPH_SPEC_URL=https://example.com/example-graph-spec.yml
```

To build a custom graph, alter the Graph Spec file. See the graph_specs directory for examples. 

TODO: explain options available in the graph spec (normalization version, source data version can be specified)
```
graphs:
  - graph_id: Example_Graph_ID
    graph_name: Example Graph
    graph_description: This is a description of what is in the graph.
    output_format: neo4j
    sources:
      - source_id: Biolink
      - source_id: HGNC
```

Install Docker to create and run the necessary containers. 

By default using docker-compose up will build every graph in your Graph Spec. It runs the command: python /ORION/Common/build_manager.py all.
```
docker-compose up
```
If you want to specify an individual graph you can override the default command with a graph id from your Spec.
```
docker-compose run --rm orion python /ORION/Common/build_manager.py Example_Graph_ID
```
To run the ORION pipeline for a single data source, you can use:
```
docker-compose run --rm orion python /ORION/Common/load_manager.py Example_Source
```
To see available arguments and a list of supported data sources:
```
python /ORION/Common/load_manager.py -h
```

#### Installing DrugCentral
In order to install these follow this pattern: 
1. Make sure you have postgres installed and running
`sudo systemctl start postgresql` or check `systemctl is-enabled postgresql`
2. Download the data
3. Extract the SQL file from the zip
4. Examine the set_up_*_env.sh file for details and load them using `source` command
5. Create the user and database
```sh
sudo -u postgres psql
CREATE USER "example-user" WITH PASSWORD 'example-pass';
CREATE DATABASE drugcentral OWNER "example-user";
\q
```
1. Go to the SQL file loaction and load the data into the server
```sh
psql -U example-user -d drugcentral -h localhost -p 5432 -f drugcentral.dump.11012023.sql
```

**PrimeKG solution to review**
```bash
# Database: Drug Central, Script: drugcentral_queries.txt, Output: drug_disease.csv
curl "https://unmtid-shinyapps.net/download/drugcentral.dump.05102023.sql.gz" -o data/drugcentral/drugcentral.dump.05102023.sql.gz
gunzip data/drugcentral/drugcentral.dump.05102023.sql.gz

# Initialize the PostgreSQL database.
module load postgresql/15.2
rm -rf /n/data1/hms/dbmi/zitnik/lab/users/an252/PrimeKG/datasets/data/drugcentral/db
initdb -D /n/data1/hms/dbmi/zitnik/lab/users/an252/PrimeKG/datasets/data/drugcentral/db
pg_ctl -D /n/data1/hms/dbmi/zitnik/lab/users/an252/PrimeKG/datasets/data/drugcentral/db -l logfile start
# Server should now be started! Check with:
pg_isready
# Create the Drug Central database.
createdb drugcentral
psql drugcentral < drugcentral.dump.05102023.sql
psql -d drugcentral -c "SELECT DISTINCT * FROM structures RIGHT JOIN (SELECT * FROM omop_relationship WHERE relationship_name IN ('indication', 'contraindication', 'off-label use')) AS drug_disease ON structures.id = drug_disease.struct_id;" -P format=csv -o drug_disease.csv

# Database: Drug Central, Script: drugcentral_feature.Rmd, Output: dc_features.csv
# TODO: run drugcentral_feature.Rmd.

```

#### Installing PHAROS
In order to install these follow this pattern: 
1. Make sure you have mysql installed and running
`sudo apt install mysql-server -y`
`sudo systemctl status mysql` and `sudo systemctl start mysql` if needed
2. Download the data
3. Extract the SQL file from the zip
4. Examine the set_up_*_env.sh file for details and load them using `source` command
5. Create the user and database
```sh
sudo mysql -u root -p
CREATE DATABASE PHAROS;
CREATE USER 'ds-user'@'localhost' IDENTIFIED BY 'ds-pass';
GRANT ALL PRIVILEGES ON PHAROS.* TO 'ds-user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```
1. Go to the SQL file loaction and load the data into the server
```sh
sudo apt install pv -y
pv TCRDv6.13.4.sql | mysql -u ds-user -p PHAROS
```


### For Developers

To add a new data source to ORION, create a new parser. Each parser extends the SourceDataLoader interface in Common/loader_interface.py.

To implement the interface you will need to write a class that fulfills the following.

Set the class level variables for the source ID and provenance: 
```
source_id: str = 'ExampleSourceID'
provenance_id: str = 'infores:example_source'
```

In initialization, call the parent init function first and pass the initialization arguments.
Then set the file names for the data file or files.
```
super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

self.data_file = 'example_file.gz'
OR
self.example_file_1 = 'example_file_1.csv'
self.example_file_2 = 'example_file_2.csv'
self.data_files = [self.example_file_1, self.example_file_2]
```

Note that self.data_path is set by the parent class and by default refers to a specific directory for the current version of that source in the storage directory.

Implement get_latest_source_version(). This function should return a string representing the latest available version of the source data.

Implement get_data(). This function should retrieve any source data files. The files should be stored with the file names specified by self.data_file or self.data_files. They should be saved in the directory specified by self.data_path.

Implement parse_data(). This function should parse the data files and populate lists of node and edge objects: self.final_node_list (kgxnode), self.final_edge_list (kgxedge).

Finally, add your source to the list of sources in Common/data_sources.py. The source ID string here should match the one specified in the new parser. Also add your source to the SOURCE_DATA_LOADER_CLASS_IMPORTS dictionary, mapping it to the new parser class.

Now you can use that source ID in a graph spec to include your new source in a graph, or as the source id using load_manager.py.

#### Testing and Troubleshooting

After you alter the codebase, or if you are experiencing issues or errors you may want to run tests:
```
docker-compose run --rm orion pytest /ORION
```