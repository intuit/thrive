#!/bin/bash

# copy data to hdfs
chmod +x hdfs_data_copy.sh
./hdfs_data_copy.sh

# run cleanup
cd ../
python runthrive.py --phase=cleanup --data-config=config/thrive_example_config.cfg --env-config=example/globalprod.cfg

# run prepare
python runthrive.py --phase=prepare --data-config=config/thrive_example_config.cfg --env-config=example/globalprod.cfg

# setup metadata
cd utils/metadata/
chmod +x md_setup.sh
./md_setup.sh -f ../../example/globalprod.cfg

# run setup phase
cd ../../
python runthrive.py --phase=setup --data-config=config/thrive_example_config.cfg --env-config=example/globalprod.cfg \
--resources=datasets/example/example-resources.zip

# run load phase
python runthrive.py --phase=load --data-config=config/thrive_example_config.cfg --env-config=example/globalprod.cfg

hive -e "use default; select * from example limit 10;"
