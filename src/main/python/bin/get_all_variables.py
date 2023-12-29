import os

# SET Environment Variables
os.environ['envn'] = "TEST"
os.environ['header'] = "True"
os.environ['inferSchema'] = "True"

# GET Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

# SET Other Variables
appName = "Prescriber Analytics"
current_path = os.getcwd()
path_dim_city = current_path + "\\..\\staging\\dimension_city"
path_fact = current_path + "\\..\\staging\\fact"

