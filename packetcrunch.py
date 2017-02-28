from pyspark import SparkContext, SparkConf, SparkFiles
from geoip2 import database, errors

#RDD of sflow strings ->
#RDD of tuples (ip, size)
#N:1 transformation
def size_by_ip(sflow):
    #Splitting the lines by commas, the result is an RDD of tuples, each tuple 
    #contains all the sflow fields of one particular packet
    result = sflow.map(lambda line: tuple(line.split(",")))
    #Forming pair of "ip - size" tuples for every packet
    result = result.flatMap(lambda packet: [(packet[10],int(packet[18])),(packet[11],int(packet[18]))])
    #Adding up the tuples so that every ip is now unique and the second tuple 
    #object corresponds to the total traffic of that IP
    result = result.reduceByKey(lambda a,b: a+b)
    result = result.sortByKey()
    return result

#RDD of tuples (ip, size) ->
#RDD of tuples (country, size)
#N:1 transformation
def size_by_country(stuple):
    #Substituting IPs with countries in the RDD
    stuple = stuple.mapPartitions(ip_to_country)
    #Adding up the tuples so that every country is mentioned only one time and 
    #the second tuple object corresponds to the total traffic of that country
    stuple = stuple.reduceByKey(lambda a,b: a+b)
    stuple = stuple.sortByKey()
    return stuple

#Iterator of tuples (ip, size) ->
#Iterator of tuples (country, size)
#1:1 transformation
def ip_to_country(stuple):
    gi = database.Reader(SparkFiles.get("GeoLite2-Country.mmdb"))
    return iter([(get_country(gi, ip[0]), ip[1]) for ip in stuple])

#String representation of IP address -> 
#String, country name
def get_country(gi, ip):
    from contextlib import suppress
    country = None
    with suppress(errors.AddressNotFoundError):
        country = gi.country(ip).country.name
    if country == None:
        country = "NOT_FOUND"
    return country

#SparkContext ->
#Saves GeoLite2 database in files of context if it's found
def find_geoip2(sc):
    import os.path
    filen = "GeoLite2-Country.mmdb"
    #First searching in the working directory
    if not os.path.isfile(filen):
        filen = os.path.join("/usr/share/GeoIP", filen)
    #If not found, searching "/usr/share/GeoIP", as established by FHS
    if not os.path.isfile(filen):
        print('GeoLite2 database was not found. Please put the database file \
                name "GeoLite2-Country.mmdb" into the working directory or \
                inside /usr/share/GeoIP.')
    return sc.addFile(filen)

#String that names the first object in JSON file, list with tuples of value pairs 
#that correspond to JSON objects ->
#Dumps the json file in the working directory based on the name passed
def output_to_json(name, json_dict):
    import json
    #Morphing it all into a list of dictionaries. Required for conversion to JSON.
    json_dict = [dict(zip([name,"sum"], key)) for key in json_dict]
    #Filename is generated based on the first argument
    fname = "result_" + name + ".csv"
    with open(fname, "wt") as f:
        json.dump(json_dict, f, sort_keys=True, indent=4)

#List of tuples containing (country, size) pairs ->
#Saves country-traffic bar chart as country_traffic.png in the working directory
def save_country_hist(country_list):
    import numpy as np
    import matplotlib.pyplot as plt
    n = len(country_list)
    X = np.arange(n)
    Y = [country[1] for country in country_list]
    Z = [country[0] for country in country_list]
    plt.bar(X, Y, color='g', linewidth=0, alpha=0.75)
    plt.xticks(X, Z, rotation='vertical', horizontalalignment='left', fontsize=2)
    plt.xlim(0, n)
    plt.yscale('symlog')
    plt.grid(True)
    plt.title('Total packet traffic by country')
    plt.xlabel('Country')
    plt.ylabel('Traffic')
    plt.savefig("country_traffic.png", dpi=300)
    plt.close()


#Establishing the Spark Context.
conf = SparkConf()
sc = SparkContext(appName="packetcrunch", conf=conf)

#Solving task 1
sflow = sc.textFile("./sflow-0118.csv")
sflow = size_by_ip(sflow).cache()
output_to_json("ip", sflow.collect()) 

#Solving task 2
find_geoip2(sc)
country_list = size_by_country(sflow).collect()
output_to_json("country", country_list) 

#We don't need Spark anymore, everything it could process already is.
sc.stop()
del sc, conf, sflow

#Solving task 3
save_country_hist(country_list)
