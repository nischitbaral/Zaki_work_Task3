# import requests
# import logging


# logger = logging.getLogger("ETL")


# class Parent:
#     def __init__(self,name,distance):
#         self.name = name
#         self.distance = distance
#     def __repr__(self):
#         return f"{self.name}: {self.distance} million km"



# def planet_data_show():
#     url = "https://api.le-systeme-solaire.net/rest/bodies/"

#     response = requests.get(url)
#     data = response.json()


#     planet_data = []

#     for body in data["bodies"]:
#         if body.get("isPlanet"):
#             distance = body.get("semimajorAxis") / 1000000
#             planet_data.append(Parent(
#                 name=body["englishName"],
#                 distance=distance
#             ))
#     return planet_data 


# def asc_sort(planet_data):

#     for i in range(1,len(planet_data)):
#         key = planet_data[i] # current element lai key ma store garne ani inserrt garne
#         j = i-1
#         while j >= 0 and key.distance < planet_data[j].distance:
#             planet_data[j+1] = planet_data[j]   
#             j -= 1
#         planet_data[j+1] = key 
#     return planet_data

# def asc_data():
#     planet_data = planet_data_show()
#     planet_asc = asc_sort(planet_data)
#     for data in planet_asc:
#         logger.info(data)

# def desc_data():
#     planet_data = planet_data_show()
#     planet_desc = asc_sort(planet_data)[::-1]  
#     for data in planet_desc:
#         logger.info(data)


































# # def desc_sort(data):
# #     return data[::-1]

# # sort_data = insertion_sort(planet_data.copy())

# # des_sort = desc_sort(sort_data)

# # for planet in sort_data:
# #     p = Parent(planet['name'], planet['distance'])
# #     print(f"Planet: {p.name}, Distance from Sun: {p.distance:} million km",'\n')


# # for planet in des_sort:
# #     p = Parent(planet['name'], planet['distance'])
# #     print(f"Planet: {p.name}, Distance from Sun: {p.distance:} million km")




















import requests
import logging
import argparse

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("ETL")

class Planet:
    def __init__(self, name, distance):
        self.name = name
        self.distance = distance
    
    def __repr__(self):
        return f"{self.name}: {self.distance} million km"

def get_planet_data():
    url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(url)
    data = response.json()

    planet_data = []
    for body in data["bodies"]:
        if body.get("isPlanet"):
            distance = body.get("semimajorAxis") / 1000000
            planet_data.append(Planet(
                name=body["englishName"],
                distance=distance
            ))
    return planet_data

def insertion_sort(planet_data, ascending=True):
    for i in range(1, len(planet_data)):
        key = planet_data[i]
        j = i - 1
        if ascending:
            while j >= 0 and key.distance < planet_data[j].distance:
                planet_data[j + 1] = planet_data[j]
                j -= 1
        else:
            while j >= 0 and key.distance > planet_data[j].distance:
                planet_data[j + 1] = planet_data[j]
                j -= 1
        planet_data[j + 1] = key
    return planet_data

def bubble_sort(planet_data, ascending=True):
    n = len(planet_data)
    for i in range(n):
        for j in range(0, n-i-1):
            if ascending:
                if planet_data[j].distance > planet_data[j+1].distance:
                    planet_data[j], planet_data[j+1] = planet_data[j+1], planet_data[j]
            else:
                if planet_data[j].distance < planet_data[j+1].distance:
                    planet_data[j], planet_data[j+1] = planet_data[j+1], planet_data[j]
    return planet_data

def main():
    parser = argparse.ArgumentParser(description="Sort planets by distance from the sun")
    parser.add_argument("--sort", choices=["apiA", "apiD"], required=True,
                       help="Sort order: apiA for ascending, apiD for descending")
    parser.add_argument("--algorithm", choices=["insertion", "bubble"], required=True,
                       help="Sorting algorithm to use")
    
    args = parser.parse_args()
    


    sort_arg = args.sort.lower()
    if sort_arg.endswith('a'):
        ascending = True
    elif sort_arg.endswith('d'):
        ascending = False
 
    planet_data = get_planet_data()
    
    
    if args.algorithm == "insertion":
        sorted_data = insertion_sort(planet_data, ascending)
    else:
        sorted_data = bubble_sort(planet_data, ascending)
    
    for data in sorted_data:
        logger.info(data)

if __name__ == "__main__":
    main()