# Set up for DNS QUERIES COLLECTOR 🛠

Documentation for setting up and launching the ETL responsible for parsing data from the **queries** file, which will be downloaded or read from the root folder of the project.

- It is recommended to use python 10.x or higher.

# Set up 📄

- 📦 **init virtual environment**:

```
python -m venv .myenv

windows: .\.myenv\Scripts\activate
linux o mac: source .myenv/bin/activate

pip install -r requirements.txt
```

- 🛠 **Run ETL**:

```
python etl.py
```
