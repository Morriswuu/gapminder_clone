import pandas as pd
import sqlite3

class CreateGapminder:
  def __init__(self):
      self.file_name =["ddf--datapoints--gdp_pcap--by--country--time.csv",
             "ddf--datapoints--pop--by--country--time.csv",
             "ddf--datapoints--lex--by--country--time.csv",
             "ddf--entities--geo--country.csv"]
      self.table_name = ["gdp_pacp", "population", "life_expectancy", "geography"]
  def import_as_dataframe(self):
      df_dict = {}
      for file_name, table_name in zip(self.file_name, self.table_name):
          file_path = f"data/{file_name}"
          df = pd.read_csv(file_path)
          df_dict[table_name] = df
      return df_dict
  def create_database(self):
      connection = sqlite3.connect("data/gapminder.db")
      df_dict = self.import_as_dataframe()
      for k, v in df_dict.items():
          v.to_sql(name=k, con=connection, index=False,if_exists="replace")
      drop_view_sql = """
      DROP VIEW IF EXISTS plotting;
      """
      create_view_sql = """
      CREATE VIEW plotting AS
      SELECT geography.name AS country_name,
             geography.world_4region AS continent,
             gdp_pacp.time AS year,
             gdp_pacp.gdp_pcap AS gdp_per_capita,
             population.pop AS population,
             life_expectancy.lex AS life_expectancy
      FROM gdp_pacp
      JOIN geography 
        ON gdp_pacp.country = geography.country
      JOIN population
        ON gdp_pacp.country = population.country AND
           gdp_pacp.time = population.time
      JOIN life_expectancy
        ON gdp_pacp.country = life_expectancy.country AND
           gdp_pacp.time = life_expectancy.time
      WHERE gdp_pacp.time < 2024;
      """
      cursor = connection.cursor()
      cursor.execute(drop_view_sql)
      cursor.execute(create_view_sql)
      connection.close()

create_gapminder = CreateGapminder()
create_gapminder.create_database()
