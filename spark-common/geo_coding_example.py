from geopy import Nominatim
# This is a geo coding example using open street data. Nominatim is a module in geo py which use openstreet data.
locator = Nominatim(user_agent="myGeocoder")
location = locator.geocode("Champ de Mars, Paris, France")

