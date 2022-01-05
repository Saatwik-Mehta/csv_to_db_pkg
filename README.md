# CSV_TO_DB_CONVERTER
It takes a CSV file as input, converts 
and save the data into a default set Database (`fileupload`).
The table name will consist of the name of the file user is uploading.
After a successful upload user can perform CRUD operations on the file data using a localhost server.
----
## Installation
```python
pip install csv-to-db-converter-saatwikmehta
```
----
### STEPS TO INSTALL & MODIFY THE PROJECT 
* Activate your python venv
* Install the requirements given:
    ```python
    pip install -r requirements.txt
    ```
* **Mysql** is the main component for storing data
  * Set env variables
  <br/>`$env:HOST= your hostname for mysql`
  <br/>`$env:USER= your username`
  <br/>`$env:PASSWORD= your password`
  
* Run server inside directory file [upload_file.py](src/csv_to_db_converter/upload_file.py)
```python
python upload_file.py
```
----
## License 
© 2022 Saatwik Mehta

This repository is licensed under the MIT license. See [LICENSE](LICENSE) for details.


