import requests
import logging


logger = logging.getLogger("ETL")


class Parent:
    def __init__(self,name,distance):
        self.name = name
        self.distance = distance
    def __repr__(self):
        return f"{self.name}: {self.distance} million km"



def planet_data_show():
    url = "https://api.le-systeme-solaire.net/rest/bodies/"

    response = requests.get(url)
    data = response.json()


    planet_data = []

    for body in data["bodies"]:
        if body.get("isPlanet"):
            distance = body.get("semimajorAxis") / 1000000
            planet_data.append(Parent(
                name=body["englishName"],
                distance=distance
            ))
    return planet_data 


def asc_sort(planet_data):

    for i in range(1,len(planet_data)):
        key = planet_data[i] # current element lai key ma store garne ani inserrt garne
        j = i-1
        while j >= 0 and key.distance < planet_data[j].distance:
            planet_data[j+1] = planet_data[j]   
            j -= 1
        planet_data[j+1] = key 
    return planet_data

def asc_data():
    planet_data = planet_data_show()
    planet_asc = asc_sort(planet_data)
    for data in planet_asc:
        logger.info(data)

def desc_data():
    planet_data = planet_data_show()
    planet_desc = asc_sort(planet_data)[::-1]  
    for data in planet_desc:
        logger.info(data)


































# def desc_sort(data):
#     return data[::-1]

# sort_data = insertion_sort(planet_data.copy())

# des_sort = desc_sort(sort_data)

# for planet in sort_data:
#     p = Parent(planet['name'], planet['distance'])
#     print(f"Planet: {p.name}, Distance from Sun: {p.distance:} million km",'\n')


# for planet in des_sort:
#     p = Parent(planet['name'], planet['distance'])
#     print(f"Planet: {p.name}, Distance from Sun: {p.distance:} million km")


