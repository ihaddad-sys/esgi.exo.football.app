from pyspark.sql import SparkSession

#To be able to call this function

#esgi.exo.football_app.main(sys.argv)

class datacleanup():





#Loading the CSV file into SparkSession
    def opencsv(self, filename):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(filename, header=True, sep=",").cache()

#Renaming the X4 and X6 collumns into "match" and "competition"
    def renamecolumns(self, df):
        return df.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")

#Selecting all needed columns in this order: match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date and casting some columns to the integer type

    def selectcolumns(self, df):
        return df.select(df.match, df.competition, df.adversaire, df.score_france.cast('int'), df.score_adversaire.cast('int'), df.penalty_france.cast('int'), df.penalty_adversaire.cast('int'), df.date)

#Filling all the null values with "0"
    def fillnull(self, df):
        return df.na.fill(0)

#Filtering data from March 1980 onwards
    def filterSinceMarch80(self, df):
        return df.filter(df.date >= '1980-03-01')

#Using all functions to 
    def cleanupfinal(self, filename):
        df_csv = self.opencsv(filename)
        df_rename = self.renamecolumns(df_csv)
        df_select = self.selectcolumns(df_rename)
        df_null = self.fillnull(df_select)
        df_cleaned = self.filterSinceMarch80(df_null)

        return df_cleaned
